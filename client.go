package varlink

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
)

type clientRequest struct {
	Method     string      `json:"method"`
	Parameters interface{} `json:"parameters"`
	Oneway     bool        `json:"oneway,omitempty"`
	More       bool        `json:"more,omitempty"`
	Upgrade    bool        `json:"upgrade,omitempty"`
}

type clientReply struct {
	Parameters json.RawMessage `json:"parameters"`
	Continues  bool            `json:"continues,omitempty"`
	Error      string          `json:"error,omitempty"`
}

// ClientError is a Varlink error returned by a service to a Client.
type ClientError struct {
	Name       string
	Parameters json.RawMessage
}

// Error implements the error interface.
func (err *ClientError) Error() string {
	return fmt.Sprintf("varlink: client call failed: %v", err.Name)
}

// Client is a Varlink client.
//
// Client methods are safe to use from multiple goroutines.
type Client struct {
	conn *conn

	mutex     sync.Mutex
	pending   []chan<- clientReply
	upgradeCh chan<- clientReply
	err       error

	readDone chan struct{}
}

// NewClient creates a Varlink client from a net.Conn.
func NewClient(conn net.Conn) *Client {
	c := &Client{
		conn:     newConn(conn),
		readDone: make(chan struct{}),
	}
	go c.readLoop()
	return c
}

// Close closes the connection.
//
// Once the connection has been hijacked it belongs to the caller of DoUpgrade,
// which must close it: this becomes a no-op.
func (c *Client) Close() error {
	if c.conn.hijacked.Load() {
		return nil
	}
	return c.conn.Close()
}

// writeRequest sends a request to the server, and registers a channel for
// a reply. For Oneway requests, ch should be nil.
func (c *Client) writeRequest(req *clientRequest, ch chan<- clientReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.err != nil {
		return c.err
	}

	if c.upgradeCh != nil {
		return fmt.Errorf("varlink: an upgrade call is already in flight")
	}

	if !req.Oneway {
		c.pending = append(c.pending, ch)
		if req.Upgrade {
			c.upgradeCh = ch
		}
	}

	if req.Parameters == nil {
		req.Parameters = struct{}{}
	}

	err := c.conn.writeMessage(req)
	if err != nil {
		c.err = err
		c.conn.Close()
		return err
	}

	return nil
}

func (c *Client) readLoop() {
	var err error
	var hijack bool
	defer func() {
		c.mutex.Lock()
		defer c.mutex.Unlock()

		if err != nil {
			c.err = err
		}

		for _, ch := range c.pending {
			close(ch)
		}
		c.pending = nil
		close(c.readDone)
	}()

	for {
		var reply clientReply
		if err = c.conn.readMessage(&reply); err != nil {
			if errors.Is(err, net.ErrClosed) {
				err = nil
			}
			break
		}

		var ch chan<- clientReply
		c.mutex.Lock()
		if len(c.pending) > 0 {
			ch = c.pending[0]
			if !reply.Continues {
				c.pending = c.pending[1:]
				if ch == c.upgradeCh {
					if reply.Error == "" {
						hijack = true
					} else {
						c.upgradeCh = nil
					}
				}
			}
		}
		c.mutex.Unlock()

		if ch == nil {
			err = fmt.Errorf("varlink: received reply without request")
			break
		}

		ch <- reply

		if hijack {
			err = ErrHijacked
			break
		}
	}
}

// Do performs a Varlink call.
//
// in is a Go value marshaled to a JSON object which contains the request
// parameters. Similarly, out will be populated with the reply parameters.
func (c *Client) Do(method string, in, out interface{}) error {
	req := clientRequest{
		Method:     method,
		Parameters: in,
	}
	cc, err := c.do(&req)
	if err != nil {
		return err
	}
	continues, err := cc.next(out)
	if continues {
		c.conn.Close()
		return fmt.Errorf("varlink: received continues=true in response to a more=false request")
	}
	return err
}

// DoMore is similar to Do, but indicates to the service that multiple replies
// are expected.
func (c *Client) DoMore(method string, in interface{}) (*ClientCall, error) {
	req := clientRequest{
		Method:     method,
		Parameters: in,
		More:       true,
	}
	return c.do(&req)
}

// DoOneway is similar to Do, but does not expect any response from the service.
func (c *Client) DoOneway(method string, in interface{}) error {
	req := clientRequest{
		Method:     method,
		Parameters: in,
		Oneway:     true,
	}
	return c.writeRequest(&req, nil)
}

// DoUpgrade is similar to Do, but requests the connection to be upgraded.
//
// If the service accepts the upgrade, the connection is taken over, giving up
// Varlink message framing: the caller must read from the returned bufio.Reader,
// and is responsible for closing the connection. Subsequent calls fail with
// ErrHijacked.
//
// The returned connection and reader are nil if the service replied with an
// error, in which case the Client remains usable.
func (c *Client) DoUpgrade(method string, in, out interface{}) (net.Conn, *bufio.Reader, error) {
	req := clientRequest{
		Method:     method,
		Parameters: in,
		Upgrade:    true,
	}
	cc, err := c.do(&req)
	if err != nil {
		return nil, nil, err
	}
	continues, err := cc.next(out)
	if continues {
		c.conn.Close()
		return nil, nil, fmt.Errorf("varlink: received continues=true in response to a more=false request")
	}
	if err != nil {
		return nil, nil, err
	}

	// the reply was successful: readLoop has stopped reading
	<-c.readDone

	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.conn.hijack()
}

func (c *Client) do(req *clientRequest) (*ClientCall, error) {
	ch := make(chan clientReply, 32)
	if err := c.writeRequest(req, ch); err != nil {
		return nil, err
	}

	return &ClientCall{
		c:  c,
		ch: ch,
	}, nil
}

// ClientCall represents an in-progress Varlink method call.
type ClientCall struct {
	c  *Client
	ch <-chan clientReply
}

// Next waits for a reply.
//
// If there are no more replies, io.EOF is returned.
func (cc *ClientCall) Next(out interface{}) error {
	if cc.ch == nil {
		return io.EOF
	}

	continues, err := cc.next(out)
	if !continues {
		cc.ch = nil
	}
	return err
}

func (cc *ClientCall) next(out interface{}) (continues bool, err error) {
	if out == nil {
		out = new(struct{})
	}

	reply, ok := <-cc.ch
	if !ok {
		return false, cc.c.err
	}

	if reply.Error != "" {
		return reply.Continues, &ClientError{Name: reply.Error, Parameters: reply.Parameters}
	}

	params := reply.Parameters
	if params == nil {
		params = json.RawMessage("{}")
	}
	return reply.Continues, json.Unmarshal(params, out)
}

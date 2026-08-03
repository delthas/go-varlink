// Package varlink implements the Varlink protocol.
//
// See https://varlink.org/
package varlink

import (
	"bufio"
	"encoding/json"
	"errors"
	"net"
	"sync/atomic"
)

// ErrHijacked is returned when the connection has been hijacked.
var ErrHijacked = errors.New("varlink: connection has been hijacked")

type conn struct {
	net.Conn

	br       *bufio.Reader
	hijacked atomic.Bool
}

func newConn(c net.Conn) *conn {
	return &conn{
		Conn: c,
		br:   bufio.NewReader(c),
	}
}

func (c *conn) hijack() (net.Conn, *bufio.Reader, error) {
	if !c.hijacked.CompareAndSwap(false, true) {
		return nil, nil, ErrHijacked
	}
	return c.Conn, c.br, nil
}

func (c *conn) writeMessage(v interface{}) error {
	if c.hijacked.Load() {
		return ErrHijacked
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	b = append(b, 0)
	_, err = c.Write(b)
	return err
}

func (c *conn) readMessage(v interface{}) error {
	b, err := c.br.ReadBytes(0)
	if err != nil {
		return err
	}
	b = b[:len(b)-1]
	return json.Unmarshal(b, v)
}

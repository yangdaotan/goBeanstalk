// implements the all protocol of beanstalk
//cousumer : watch, ignore, reserve, reserve-with-time, release, delete, bury, touch
//productor: Use , Put
//other : peek, peek-ready, peek-delayed, peek-buried, kick,
//        list-tubes, list-tube-used, list-tubes-watched, pause-tube, stats, stats-jobs, stats-tube, quit

package goBeanstalk

import (
	"container/list"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"time"
)

// A Conn represents a connection to a beanstalk server.
type Conn struct {
	c    *textproto.Conn
	used string //tube of used
	// watched []string
	watched *list.List //tubes of the watched
}

// request/response of Pipeline
type Req struct {
	id uint
	op string
}

var (
	space      = []byte{' '}
	crnl       = []byte{'\r', '\n'}
	yamlHead   = []byte{'-', '-', '-', '\n'}
	nl         = []byte{'\n'}
	colonSpace = []byte{':', ' '}
	minusSpace = []byte{'-', ' '}
)

// NewConn returns a new Conn using conn for I/O.
func NewConn(conn io.ReadWriteCloser) *Conn {
	c := new(Conn)
	c.c = textproto.NewConn(conn)
	c.used = "default"
	c.watched = list.New()
	c.watched.PushBack("default")
	// c.watched = append(c.watched, "default")
	return c
}

// Dial connects to the given address on the given network using net.Dial
// and then returns a new Conn for the connection.
func Dial(network, addr string) (*Conn, error) {
	c, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return NewConn(c), nil
}

func (c *Conn) GetWatch() *list.List {
	return c.watched
}

func (c *Conn) Close() error {
	return c.c.Close()
}

//Write wirte the cmd and body to server
func (c *Conn) Write(body []byte, op string, args ...interface{}) (Req, error) {
	r := Req{c.c.Next(), op}
	c.c.StartRequest(r.id)

	if body != nil {
		args = append(args, len(body))
	}

	c.WriteLine(string(op), args...)

	if body != nil {
		c.c.W.Write(body)
		c.c.W.Write(crnl)
	}

	err := c.c.W.Flush()
	if err != nil {
		return Req{}, ConnError{c, op, err}
	}

	c.c.EndRequest(r.id)

	return r, nil
}

//write cmd
func (c *Conn) WriteLine(cmd string, args ...interface{}) {
	io.WriteString(c.c.W, cmd)
	for _, a := range args {
		c.c.W.Write(space)
		fmt.Fprint(c.c.W, a)
	}
	c.c.W.Write(crnl)
}

// read the response from server and then format
func (c *Conn) Read(r Req, readBody bool, format string, a ...interface{}) (body []byte, err error) {
	c.c.StartResponse(r.id)
	defer c.c.EndResponse(r.id)
	line, err := c.c.ReadLine()
	if err != nil {
		return nil, ConnError{c, r.op, err}
	}

	toScan := line
	if readBody {
		var size int
		toScan, size, err = ParseSize(toScan)
		if err != nil {
			return nil, ConnError{c, r.op, err}
		}
		body = make([]byte, size+2)
		_, err = io.ReadFull(c.c.R, body)
		if err != nil {
			return nil, ConnError{c, r.op, err}
		}
		body = body[:size]
	}

	_, err = fmt.Sscanf(toScan, format, a...)
	if err != nil {
		return nil, ConnError{c, r.op, FindRespError(toScan)}
	}

	return body, nil
}

//productor operator
func (c *Conn) Use(tubeName string) error {
	r, err := c.Write(nil, "use", tubeName)
	if err != nil {
		return err
	}
	_, err = c.Read(r, false, "USING")
	if err == nil {
		c.used = tubeName
	}
	return err
}

func (c *Conn) Put(body []byte, pri uint64, delay, ttr time.Duration) (id uint64, err error) {
	r, err := c.Write(body, "put", pri, dur(delay), dur(ttr))
	if err != nil {
		return 0, err
	}
	_, err = c.Read(r, false, "INSERTED %d", &id)
	if err != nil {
		return 0, err
	}
	return id, nil
}

//consumer operator
func (c *Conn) Watch(tubeName string) (n int, err error) {
	r, err := c.Write(nil, "watch", tubeName)
	if err != nil {
		return 0, err
	}
	_, err = c.Read(r, false, "WATCHING %d", &n)
	if err != nil {
		return 0, err
	}
	// c.watched = append(c.watched, "tubeName")
	c.watched.PushBack(tubeName)
	return n, nil
}

func (c *Conn) Ignore(tubeName string) (n int, err error) {
	r, err := c.Write(nil, "ignore", tubeName)
	if err != nil {
		return 0, err
	}
	_, err = c.Read(r, false, "WATCHING %d", &n)
	if err != nil {
		return 0, err
	}
	for e := c.watched.Front(); e != nil; e = e.Next() {
		if e.Value == tubeName {
			c.watched.Remove(e)
			break
		}
	}
	return n, nil
}

func (c *Conn) Reserve() (id uint64, body []byte, err error) {
	r, err := c.Write(nil, "reserve")
	if err != nil {
		return 0, nil, err
	}
	body, err = c.Read(r, true, "RESERVED %d", &id)
	if err != nil {
		return 0, nil, err
	}
	return id, body, nil
}

func (c *Conn) ReserveWithTimeout(timeout time.Duration) (id uint64, body []byte, err error) {
	r, err := c.Write(nil, "reserve-with-timeout", dur(timeout))
	if err != nil {
		return 0, nil, err
	}
	body, err = c.Read(r, true, "RESERVED %d", &id)
	if err != nil {
		return 0, nil, err
	}
	return id, body, nil
}

func (c *Conn) Release(id uint64, pri uint32, delay time.Duration) error {
	r, err := c.Write(nil, "release", id, pri, dur(delay))
	if err != nil {
		return err
	}
	_, err = c.Read(r, false, "RELEASED")
	return err
}

func (c *Conn) Delete(id uint64) error {
	r, err := c.Write(nil, "delete", id)
	if err != nil {
		return err
	}
	_, err = c.Read(r, false, "DELETED")
	return err
}

func (c *Conn) Bury(id uint64, pri uint32) error {
	r, err := c.Write(nil, "bury", id, pri)
	if err != nil {
		return err
	}
	_, err = c.Read(r, false, "BURIED")
	return err
}

func (c *Conn) Touch(id uint64) error {
	r, err := c.Write(nil, "touch", id)
	if err != nil {
		return err
	}
	_, err = c.Read(r, false, "TOUCHED")
	return err
}

// other operation
func (c *Conn) Peek(id uint64) (bytes int, body []byte, err error) {
	r, err := c.Write(nil, "peek", id)
	if err != nil {
		return 0, nil, err
	}
	body, err = c.Read(r, true, "FOUND %d", &bytes)
	if err != nil {
		return 0, nil, err
	}
	return bytes, body, err
}

func (c *Conn) PeekReady() (id uint64, body []byte, err error) {
	r, err := c.Write(nil, "peek-ready")
	if err != nil {
		return 0, nil, err
	}
	body, err = c.Read(r, true, "FOUND %d", &id)
	if err != nil {
		return 0, nil, err
	}
	return id, body, nil
}

func (c *Conn) PeekDelayed() (id uint64, body []byte, err error) {
	r, err := c.Write(nil, "peek-delayed")
	if err != nil {
		return 0, nil, err
	}
	body, err = c.Read(r, true, "FOUND %d", &id)
	if err != nil {
		return 0, body, nil
	}
	return id, body, nil
}

func (c *Conn) PeekBuried() (id uint64, body []byte, err error) {
	r, err := c.Write(nil, "peek-buried,")
	if err != nil {
		return 0, nil, err
	}
	body, err = c.Read(r, true, "FOUND %d", &id)
	if err != nil {
		return 0, body, nil
	}
	return id, body, nil
}

func (c *Conn) Kick(bound int) (n int, err error) {
	r, err := c.Write(nil, "kick", bound)
	if err != nil {
		return 0, err
	}
	_, err = c.Read(r, false, "KICKED %d", &n)
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (c *Conn) ListTubes() ([]string, error) {
	r, err := c.Write(nil, "list-tubes")
	if err != nil {
		return nil, err
	}
	body, err := c.Read(r, true, "OK")
	return ParseList(body), err
}

func (c *Conn) ListTubeUsed() (tubeName string, err error) {
	r, err := c.Write(nil, "list-tube-used")
	if err != nil {
		return "", err
	}
	_, err = c.Read(r, false, "USING %s", &tubeName)
	if err != nil {
		return "", err
	}
	return tubeName, nil
}

func (c *Conn) ListTubesWatched() ([]string, error) {
	r, err := c.Write(nil, "list-tubes-watched")
	if err != nil {
		return nil, err
	}
	body, err := c.Read(r, true, "OK")
	return ParseList(body), err
}

func (c *Conn) PauseTube(tubeName string, delay time.Duration) error {
	r, err := c.Write(nil, "pause-tube", tubeName, dur(delay))
	if err != nil {
		return err
	}
	_, err = c.Read(r, false, "PAUSE")
	if err != nil {
		return err
	}
	return nil
}

func (c *Conn) Stats() (map[string]string, error) {
	r, err := c.Write(nil, "stats")
	if err != nil {
		return nil, err
	}
	body, err := c.Read(r, true, "OK")
	return ParseDict(body), err
}

func (c *Conn) StatsJob(id uint64) (map[string]string, error) {
	r, err := c.Write(nil, "stats-job", id)
	if err != nil {
		return nil, err
	}
	body, err := c.Read(r, true, "OK")
	return ParseDict(body), err
}

func (c *Conn) StatsTube(tubeName string) (map[string]string, error) {
	r, err := c.Write(nil, "stats-tube", tubeName)
	if err != nil {
		return nil, err
	}
	body, err := c.Read(r, false, "OK")
	return ParseDict(body), err
}

func (c *Conn) Quit() error {
	_, err := c.Write(nil, "quit")
	if err != nil {
		return err
	}
	return nil
}

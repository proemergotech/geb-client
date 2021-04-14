package test

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/proemergotech/errors"
)

type Proxy struct {
	targetHost string
	targetPort int
	addr       *net.TCPAddr
	listener   net.Listener
	conns      []net.Conn
	m          *sync.Mutex
}

func NewProxy(targetHost string, targetPort int) *Proxy {
	p := &Proxy{
		targetHost: targetHost,
		targetPort: targetPort,
		m:          &sync.Mutex{},
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	p.addr = l.Addr().(*net.TCPAddr)
	l.Close()

	return p
}

func (p *Proxy) Port() int {
	return p.addr.Port
}

func (p *Proxy) Host() string {
	return p.addr.IP.String()
}

func (p *Proxy) Start() error {
	p.m.Lock()
	defer p.m.Unlock()

	l, err := net.Listen("tcp", p.addr.String())
	if err != nil {
		return errors.WithStack(err)
	}

	p.listener = l

	go func() {
		for {
			ic, err := l.Accept()
			if err != nil {
				if strings.Contains(err.Error(), "use of closed network connection") {
					return
				}
				panic(err)
			}

			oc, err := net.Dial("tcp", fmt.Sprintf("%v:%v", p.targetHost, p.targetPort))
			if err != nil {
				log.Printf("%+v %v", err, fmt.Sprintf("%v:%v", p.targetHost, p.targetPort))
				ic.Close()
				return
			}

			p.m.Lock()
			p.conns = append(p.conns, ic, oc)
			p.m.Unlock()

			go func() {
				for {
					buff := make([]byte, 0xffff)
					n, err := ic.Read(buff)
					if err != nil {
						return
					}

					_, err = oc.Write(buff[:n])
					if err != nil {
						return
					}
				}
			}()

			go func() {
				for {
					buff := make([]byte, 0xffff)
					n, err := oc.Read(buff)
					if err != nil {
						return
					}

					_, err = ic.Write(buff[:n])
					if err != nil {
						return
					}
				}
			}()
		}
	}()

	return nil
}

func (p *Proxy) Stop() {
	p.m.Lock()
	defer p.m.Unlock()

	p.listener.Close()
	p.listener = nil
	for _, c := range p.conns {
		c.Close()
	}

	p.conns = nil
}

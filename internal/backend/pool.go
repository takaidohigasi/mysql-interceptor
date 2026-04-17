package backend

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
)

type Pool struct {
	cfg    config.BackendConfig
	tlsCfg config.BackendSideTLSConfig
	conns  chan *client.Conn
}

func NewPool(cfg config.BackendConfig, tlsCfg config.BackendSideTLSConfig, size int) *Pool {
	return &Pool{
		cfg:    cfg,
		tlsCfg: tlsCfg,
		conns:  make(chan *client.Conn, size),
	}
}

func (p *Pool) Get() (*client.Conn, error) {
	select {
	case conn := <-p.conns:
		if err := conn.Ping(); err != nil {
			conn.Close()
			return p.newConn()
		}
		return conn, nil
	default:
		return p.newConn()
	}
}

func (p *Pool) Put(conn *client.Conn) {
	if conn == nil {
		return
	}
	select {
	case p.conns <- conn:
	default:
		conn.Close()
	}
}

func (p *Pool) Close() {
	close(p.conns)
	for conn := range p.conns {
		conn.Close()
	}
}

func (p *Pool) newConn() (*client.Conn, error) {
	conn, err := Connect(p.cfg, p.tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("pool: new connection: %w", err)
	}
	return conn, nil
}

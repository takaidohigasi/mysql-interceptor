package backend

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/packet"
)

func pipeConn(t *testing.T) (*client.Conn, net.Conn) {
	t.Helper()
	clientSide, serverSide := net.Pipe()
	t.Cleanup(func() { clientSide.Close(); serverSide.Close() })
	return &client.Conn{Conn: packet.NewConn(clientSide)}, serverSide
}

func assertClosed(t *testing.T, conn *client.Conn) {
	t.Helper()
	_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := conn.Write([]byte{0}); err == nil {
		t.Fatal("connection still writable after Quit")
	}
}

// Quit must put a COM_QUIT packet on the wire before the socket is closed,
// so the server records a normal disconnect instead of an aborted client.
func TestQuitSendsComQuitThenCloses(t *testing.T) {
	conn, peer := pipeConn(t)

	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 5)
		n, _ := io.ReadFull(peer, buf)
		got <- buf[:n]
	}()

	Quit(conn)

	select {
	case pkt := <-got:
		want := []byte{1, 0, 0, 0, mysql.COM_QUIT}
		if string(pkt) != string(want) {
			t.Fatalf("peer read %v, want COM_QUIT packet %v", pkt, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("peer never received COM_QUIT")
	}
	assertClosed(t, conn)
}

// A peer that has already gone away makes the COM_QUIT write fail; the
// socket must still be closed (go-mysql's Quit() returns early without
// closing in that case).
func TestQuitClosesWhenWriteFails(t *testing.T) {
	conn, peer := pipeConn(t)
	peer.Close()

	Quit(conn)
	assertClosed(t, conn)
}

// A peer that never reads must not pin teardown: the write is bounded by
// QuitTimeout and the socket is closed afterwards.
func TestQuitDoesNotBlockOnStalledPeer(t *testing.T) {
	conn, _ := pipeConn(t)

	start := time.Now()
	Quit(conn)
	if elapsed := time.Since(start); elapsed > QuitTimeout+500*time.Millisecond {
		t.Fatalf("Quit took %v, want <= %v", elapsed, QuitTimeout)
	}
	assertClosed(t, conn)
}

func TestQuitNilConn(t *testing.T) {
	Quit(nil) // must not panic
}

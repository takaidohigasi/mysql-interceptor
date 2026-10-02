package backend

import (
	"time"

	"github.com/go-mysql-org/go-mysql/client"
)

// QuitTimeout bounds the COM_QUIT write in Quit so a wedged socket cannot
// pin session teardown.
const QuitTimeout = time.Second

// Quit closes a backend connection gracefully. go-mysql's Close() is a bare
// TCP close, and MySQL/TiDB count a client that disappears without
// COM_QUIT as an aborted connection (TiDB: tidb_server_disconnection_total
// {result="error"}), so every proxy-initiated teardown was reported there
// as an error. Quit sends COM_QUIT first and closes the socket regardless
// of the outcome; go-mysql's own Quit() leaks the socket when the write
// fails, which is why it is wrapped here.
func Quit(conn *client.Conn) {
	if conn == nil {
		return
	}
	_ = conn.SetWriteDeadline(time.Now().Add(QuitTimeout))
	if err := conn.Quit(); err != nil {
		_ = conn.Close()
	}
}

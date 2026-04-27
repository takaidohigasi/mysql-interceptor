package proxy

import (
	"testing"

	"github.com/takaidohigasi/mysql-interceptor/internal/config"
)

// TestMaxConnectionsCapacity verifies that NewProxyServer sizes the
// connection-slot semaphore from cfg.Proxy.MaxConnections, falling back
// to 1000 when the config is 0 or negative.
func TestMaxConnectionsCapacity(t *testing.T) {
	cases := []struct {
		name string
		set  int
		want int
	}{
		{"explicit 50", 50, 50},
		{"zero falls back to default", 0, 1000},
		{"negative falls back to default", -5, 1000},
		{"explicit 1", 1, 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{
				Proxy: config.ProxyConfig{
					ListenAddr:     "127.0.0.1:0",
					MaxConnections: tc.set,
					Users:          []config.UserConfig{{Username: "u", Password: "p"}},
				},
				Backend: config.BackendConfig{Addr: "127.0.0.1:3306"},
			}
			srv, err := NewProxyServer(cfg, nil, nil)
			if err != nil {
				t.Fatalf("NewProxyServer: %v", err)
			}
			got := cap(srv.connSlots)
			if got != tc.want {
				t.Errorf("cap(connSlots) = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestUsersWiring verifies that NewProxyServer rejects empty users and
// otherwise builds the auth handler and password map from cfg.Proxy.Users.
func TestUsersWiring(t *testing.T) {
	t.Run("empty users rejected", func(t *testing.T) {
		cfg := &config.Config{
			Proxy:   config.ProxyConfig{ListenAddr: "127.0.0.1:0"},
			Backend: config.BackendConfig{Addr: "127.0.0.1:3306"},
		}
		if _, err := NewProxyServer(cfg, nil, nil); err == nil {
			t.Fatal("expected error for empty proxy.users, got nil")
		}
	})

	t.Run("users populated", func(t *testing.T) {
		cfg := &config.Config{
			Proxy: config.ProxyConfig{
				ListenAddr: "127.0.0.1:0",
				Users: []config.UserConfig{
					{Username: "alice", Password: "alice-pw"},
					{Username: "bob", Password: "bob-pw"},
				},
			},
			Backend: config.BackendConfig{Addr: "127.0.0.1:3306"},
		}
		srv, err := NewProxyServer(cfg, nil, nil)
		if err != nil {
			t.Fatalf("NewProxyServer: %v", err)
		}
		if srv.authHandler == nil {
			t.Fatal("expected authHandler to be set")
		}
		if got := srv.userPasswords["alice"]; got != "alice-pw" {
			t.Errorf("userPasswords[alice] = %q, want alice-pw", got)
		}
		if got := srv.userPasswords["bob"]; got != "bob-pw" {
			t.Errorf("userPasswords[bob] = %q, want bob-pw", got)
		}
		if _, ok := srv.userPasswords["mallory"]; ok {
			t.Error("unexpected user mallory in userPasswords")
		}
	})
}

// TestRedactArgs verifies the local redact helper.
func TestRedactArgs(t *testing.T) {
	args := []interface{}{"secret-password", 42, nil, []byte("token")}
	got := redact(args)
	if len(got) != len(args) {
		t.Fatalf("expected %d elements, got %d", len(args), len(got))
	}
	for i, v := range got {
		if s, ok := v.(string); !ok || s != "<redacted>" {
			t.Errorf("got[%d] = %v, want \"<redacted>\"", i, v)
		}
	}
	// Original slice should be untouched.
	if args[0] != "secret-password" {
		t.Error("redact mutated the input slice")
	}
}

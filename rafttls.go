package persist

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/hashicorp/raft"
)

// LoadRaftTLS builds a mutual TLS config. The certificate is presented as both
// the server and the client certificate. caFile is the trust anchor for peers.
func LoadRaftTLS(certFile, keyFile, caFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" || caFile == "" {
		return nil, fmt.Errorf("persist: raft TLS requires a certificate, key, and CA")
	}
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("persist: raft CA %s has no certificates", caFile)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{pair},
		ClientCAs:    pool,
		RootCAs:      pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

func loopbackHost(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

type tlsStreamLayer struct {
	advertise net.Addr
	listener  net.Listener
	config    *tls.Config
}

func newTLSTransport(bind string, advertise net.Addr, cfg *tls.Config) (*raft.NetworkTransport, error) {
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, err
	}
	stream := &tlsStreamLayer{
		advertise: advertise,
		listener:  ln,
		config:    cfg,
	}
	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok || addr.IP == nil || addr.IP.IsUnspecified() {
		ln.Close()
		return nil, fmt.Errorf("persist: raft advertise address is not usable")
	}
	return raft.NewNetworkTransport(stream, 3, 10*time.Second, os.Stderr), nil
}

func (t *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return tls.DialWithDialer(dialer, "tcp", string(address), t.config)
}

func (t *tlsStreamLayer) Accept() (net.Conn, error) {
	conn, err := t.listener.Accept()
	if err != nil {
		return nil, err
	}
	return tls.Server(conn, t.config), nil
}

func (t *tlsStreamLayer) Close() error {
	return t.listener.Close()
}

func (t *tlsStreamLayer) Addr() net.Addr {
	if t.advertise != nil {
		return t.advertise
	}
	return t.listener.Addr()
}

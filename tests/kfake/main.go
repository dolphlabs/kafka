// In-process Kafka clusters (franz-go's kfake) for tests/cluster and the
// client tests that follow it: a plain 3-broker cluster, one that requires
// SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512), and one that requires TLS
// and SCRAM-SHA-512. It writes the CA it made for the TLS cluster to the
// directory given as its argument, prints one line
//
//	plain=host:port,... sasl=host:port tls=host:port ca=/path/ca.pem
//
// and runs until its standard input closes.
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func addrs(c *kfake.Cluster) string {
	return strings.Join(c.ListenAddrs(), ",")
}

// A CA and a server certificate for 127.0.0.1 and localhost.
func tlsConfig(dir string) (*tls.Config, string) {
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "kafka test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), IsCA: true,
		KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &caKey.PublicKey, caKey)
	must(err)
	caCert, _ := x509.ParseCertificate(caDER)
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, leaf, caCert, &key.PublicKey, caKey)
	must(err)
	caPath := filepath.Join(dir, "ca.pem")
	must(os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o644))
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	return &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}, caPath
}

func main() {
	dir := os.TempDir()
	if len(os.Args) > 1 {
		dir = os.Args[1]
	}
	plain, err := kfake.NewCluster(kfake.NumBrokers(3), kfake.ClusterID("slang-kafka-test"),
		kfake.SeedTopics(3, "seeded"), kfake.DefaultNumPartitions(3))
	must(err)
	defer plain.Close()
	sasl, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.EnableSASL(),
		kfake.Superuser("PLAIN", "plain-user", "plain-pass"),
		kfake.Superuser("SCRAM-SHA-256", "scram256", "pass-256"),
		kfake.Superuser("SCRAM-SHA-512", "scram512", "pass-512"))
	must(err)
	defer sasl.Close()
	cfg, caPath := tlsConfig(dir)
	secure, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.TLS(cfg), kfake.EnableSASL(),
		kfake.Superuser("SCRAM-SHA-512", "app", "app-secret"))
	must(err)
	defer secure.Close()
	fmt.Printf("plain=%s sasl=%s tls=%s ca=%s\n", addrs(plain), addrs(sasl), addrs(secure), caPath)
	io.Copy(io.Discard, os.Stdin)
}

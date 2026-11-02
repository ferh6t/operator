/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package postgres

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

// ErrHostKeyMismatch means a member presented a different SSH host key than
// the one recorded on first connect.
var ErrHostKeyMismatch = errors.New("SSH host key changed")

// Executor runs shell commands on a member as root.
type Executor interface {
	// Run runs cmd, feeding it stdin when not nil, and returns its stdout.
	// It checks node.HostKey, or records it there when empty.
	Run(ctx context.Context, node *Node, cmd string, stdin []byte) (string, error)
}

// SSHExecutor is an Executor that connects over SSH with one private key.
type SSHExecutor struct {
	Signer ssh.Signer
}

// handshakeTimeout bounds connecting and the SSH handshake.
const handshakeTimeout = 20 * time.Second

// Run implements Executor.
func (e *SSHExecutor) Run(ctx context.Context, node *Node, cmd string, stdin []byte) (string, error) {
	addr := net.JoinHostPort(node.PublicIP, "22")
	expected := node.HostKey
	var seen string
	cfg := &ssh.ClientConfig{
		User: "root",
		Auth: []ssh.AuthMethod{ssh.PublicKeys(e.Signer)},
		HostKeyCallback: func(_ string, _ net.Addr, key ssh.PublicKey) error {
			seen = ssh.FingerprintSHA256(key)
			if expected != "" && seen != expected {
				return fmt.Errorf("%w on %s: got %s, recorded %s", ErrHostKeyMismatch, node.Name, seen, expected)
			}
			return nil
		},
		Timeout: handshakeTimeout,
	}

	dialer := net.Dialer{Timeout: handshakeTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return "", fmt.Errorf("connecting to %s: %w", node.Name, err)
	}
	_ = conn.SetDeadline(time.Now().Add(handshakeTimeout))
	c, chans, reqs, err := ssh.NewClientConn(conn, addr, cfg)
	if err != nil {
		_ = conn.Close()
		return "", fmt.Errorf("SSH handshake with %s: %w", node.Name, err)
	}
	_ = conn.SetDeadline(time.Time{})
	client := ssh.NewClient(c, chans, reqs)
	defer func() { _ = client.Close() }()
	if expected == "" {
		node.HostKey = seen
	}

	// Closing the client aborts the command when ctx ends.
	stop := context.AfterFunc(ctx, func() { _ = client.Close() })
	defer stop()

	session, err := client.NewSession()
	if err != nil {
		return "", fmt.Errorf("opening SSH session on %s: %w", node.Name, err)
	}
	defer func() { _ = session.Close() }()

	var stdout, stderr bytes.Buffer
	session.Stdout = &stdout
	session.Stderr = &stderr
	if stdin != nil {
		session.Stdin = bytes.NewReader(stdin)
	}
	if err := session.Run(cmd); err != nil {
		if ctx.Err() != nil {
			return stdout.String(), fmt.Errorf("command on %s: %w", node.Name, ctx.Err())
		}
		return stdout.String(), fmt.Errorf("command on %s failed: %w: %s", node.Name, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

// GenerateKey creates an ed25519 key pair. It returns the private key in
// OpenSSH PEM format and the public key as an authorized_keys line.
func GenerateKey() (privatePEM []byte, authorizedKey string, err error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, "", fmt.Errorf("generating SSH key: %w", err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "operator")
	if err != nil {
		return nil, "", fmt.Errorf("encoding SSH private key: %w", err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		return nil, "", fmt.Errorf("encoding SSH public key: %w", err)
	}
	return pem.EncodeToMemory(block), strings.TrimSpace(string(ssh.MarshalAuthorizedKey(sshPub))) + " operator", nil
}

// GeneratePassword returns a random password of letters and digits.
func GeneratePassword() string {
	return rand.Text() + rand.Text()
}

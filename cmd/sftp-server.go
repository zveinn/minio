// Copyright (c) 2015-2023 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"bytes"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/minio/minio/internal/logger"
	xsftp "github.com/minio/pkg/v2/sftp"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

type sftpLogger struct{}

func (s *sftpLogger) Info(tag xsftp.LogType, msg string) {
	logger.Info(msg)
}

const (
	kexAlgoDH1SHA1                = "diffie-hellman-group1-sha1"
	kexAlgoDH14SHA1               = "diffie-hellman-group14-sha1"
	kexAlgoDH14SHA256             = "diffie-hellman-group14-sha256"
	kexAlgoDH16SHA512             = "diffie-hellman-group16-sha512"
	kexAlgoECDH256                = "ecdh-sha2-nistp256"
	kexAlgoECDH384                = "ecdh-sha2-nistp384"
	kexAlgoECDH521                = "ecdh-sha2-nistp521"
	kexAlgoCurve25519SHA256LibSSH = "curve25519-sha256@libssh.org"
	kexAlgoCurve25519SHA256       = "curve25519-sha256"

	chacha20Poly1305ID = "chacha20-poly1305@openssh.com"
	gcm256CipherID     = "aes256-gcm@openssh.com"
	aes128cbcID        = "aes128-cbc"
	tripledescbcID     = "3des-cbc"
)

// https://cs.opensource.google/go/x/crypto/+/refs/tags/v0.22.0:ssh/common.go;l=46
// preferredKexAlgos specifies the default preference for key-exchange
// algorithms in preference order. The diffie-hellman-group16-sha512 algorithm
// is disabled by default because it is a bit slower than the others.
var preferredKexAlgos = []string{
	kexAlgoCurve25519SHA256, kexAlgoCurve25519SHA256LibSSH,
	kexAlgoECDH256, kexAlgoECDH384, kexAlgoECDH521,
	kexAlgoDH14SHA256, kexAlgoDH14SHA1,
}

// supportedKexAlgos specifies the supported key-exchange algorithms in
// preference order.
// https://cs.opensource.google/go/x/crypto/+/refs/tags/v0.22.0:ssh/common.go;l=44
var supportedKexAlgos = []string{
	kexAlgoCurve25519SHA256, kexAlgoCurve25519SHA256LibSSH,
	// P384 and P521 are not constant-time yet, but since we don't
	// reuse ephemeral keys, using them for ECDH should be OK.
	kexAlgoECDH256, kexAlgoECDH384, kexAlgoECDH521,
	kexAlgoDH14SHA256, kexAlgoDH16SHA512, kexAlgoDH14SHA1,
	kexAlgoDH1SHA1,
}

// supportedPubKeyAuthAlgos specifies the supported client public key
// authentication algorithms. Note that this doesn't include certificate types
// since those use the underlying algorithm. This list is sent to the client if
// it supports the server-sig-algs extension. Order is irrelevant.
// https://cs.opensource.google/go/x/crypto/+/refs/tags/v0.22.0:ssh/common.go;l=142
var supportedPubKeyAuthAlgos = []string{
	ssh.KeyAlgoED25519,
	ssh.KeyAlgoSKED25519, ssh.KeyAlgoSKECDSA256,
	ssh.KeyAlgoECDSA256, ssh.KeyAlgoECDSA384, ssh.KeyAlgoECDSA521,
	ssh.KeyAlgoRSASHA256, ssh.KeyAlgoRSASHA512, ssh.KeyAlgoRSA,
	ssh.KeyAlgoDSA,
}

// supportedCiphers lists ciphers we support but might not recommend.
// https://cs.opensource.google/go/x/crypto/+/refs/tags/v0.22.0:ssh/common.go;l=28
var supportedCiphers = []string{
	"aes128-ctr", "aes192-ctr", "aes256-ctr",
	"aes128-gcm@openssh.com", gcm256CipherID,
	chacha20Poly1305ID,
	"arcfour256", "arcfour128", "arcfour",
	aes128cbcID,
	tripledescbcID,
}

// preferredCiphers specifies the default preference for ciphers.
// https://cs.opensource.google/go/x/crypto/+/refs/tags/v0.22.0:ssh/common.go;l=37
var preferredCiphers = []string{
	"aes128-gcm@openssh.com", gcm256CipherID,
	chacha20Poly1305ID,
	"aes128-ctr", "aes192-ctr", "aes256-ctr",
}

// supportedMACs specifies a default set of MAC algorithms in preference order.
// This is based on RFC 4253, section 6.4, but with hmac-md5 variants removed
// because they have reached the end of their useful life.
// https://cs.opensource.google/go/x/crypto/+/refs/tags/v0.22.0:ssh/common.go;l=85
var supportedMACs = []string{
	"hmac-sha2-256-etm@openssh.com", "hmac-sha2-512-etm@openssh.com", "hmac-sha2-256", "hmac-sha2-512", "hmac-sha1", "hmac-sha1-96",
}

func (s *sftpLogger) Error(tag xsftp.LogType, err error) {
	switch tag {
	case xsftp.AcceptNetworkError:
		sftpLogOnceIf(context.Background(), err, "accept-limit-sftp")
	case xsftp.AcceptChannelError:
		sftpLogOnceIf(context.Background(), err, "accept-channel-sftp")
	case xsftp.SSHKeyExchangeError:
		sftpLogOnceIf(context.Background(), err, "key-exchange-sftp")
	default:
		sftpLogOnceIf(context.Background(), err, "unknown-error-sftp")
	}
}

func filterAlgos(arg string, want []string, allowed []string) []string {
	var filteredAlgos []string
	found := false
	for _, algo := range want {
		if len(algo) == 0 {
			continue
		}
		for _, allowedAlgo := range allowed {
			algo := strings.ToLower(strings.TrimSpace(algo))
			if algo == allowedAlgo {
				filteredAlgos = append(filteredAlgos, algo)
				found = true
				break
			}
		}
		if !found {
			logger.Fatal(fmt.Errorf("unknown algorithm %q passed to --sftp=%s\nValid algorithms: %v", algo, arg, strings.Join(allowed, ", ")), "unable to start SFTP server")
		}
	}
	if len(filteredAlgos) == 0 {
		logger.Fatal(fmt.Errorf("no valid algorithms passed to --sftp=%s\nValid algorithms: %v", arg, strings.Join(allowed, ", ")), "unable to start SFTP server")
	}
	return filteredAlgos
}

func startSFTPServer(args []string) {
	var (
		port          int
		publicIP      string
		sshPrivateKey string
		userCaKeyFile string
	)
	allowPubKeys := supportedPubKeyAuthAlgos
	allowKexAlgos := preferredKexAlgos
	allowCiphers := preferredCiphers
	allowMACs := supportedMACs
	var err error
	for _, arg := range args {
		tokens := strings.SplitN(arg, "=", 2)
		if len(tokens) != 2 {
			logger.Fatal(fmt.Errorf("invalid arguments passed to --sftp=%s", arg), "unable to start SFTP server")
		}
		switch tokens[0] {
		case "address":
			host, portStr, err := net.SplitHostPort(tokens[1])
			if err != nil {
				logger.Fatal(fmt.Errorf("invalid arguments passed to --sftp=%s (%v)", arg, err), "unable to start SFTP server")
			}
			port, err = strconv.Atoi(portStr)
			if err != nil {
				logger.Fatal(fmt.Errorf("invalid arguments passed to --sftp=%s (%v)", arg, err), "unable to start SFTP server")
			}
			if port < 1 || port > 65535 {
				logger.Fatal(fmt.Errorf("invalid arguments passed to --sftp=%s, (port number must be between 1 to 65535)", arg), "unable to start SFTP server")
			}
			publicIP = host
		case "ssh-private-key":
			sshPrivateKey = tokens[1]
		case "pub-key-algos":
			allowPubKeys = filterAlgos(arg, strings.Split(tokens[1], ","), supportedPubKeyAuthAlgos)
		case "kex-algos":
			allowKexAlgos = filterAlgos(arg, strings.Split(tokens[1], ","), supportedKexAlgos)
		case "cipher-algos":
			allowCiphers = filterAlgos(arg, strings.Split(tokens[1], ","), supportedCiphers)
		case "mac-algos":
			allowMACs = filterAlgos(arg, strings.Split(tokens[1], ","), supportedMACs)
		case "trusted-user-ca-key":
			userCaKeyFile = tokens[1]
		}
	}

	if port == 0 {
		port = 8022 // Default SFTP port, since no port was given.
	}

	if sshPrivateKey == "" {
		logger.Fatal(fmt.Errorf("invalid arguments passed, private key file is mandatory for --sftp='ssh-private-key=path/to/id_ecdsa'"), "unable to start SFTP server")
	}

	privateBytes, err := os.ReadFile(sshPrivateKey)
	if err != nil {
		logger.Fatal(fmt.Errorf("invalid arguments passed, private key file is not accessible: %v", err), "unable to start SFTP server")
	}

	private, err := ssh.ParsePrivateKey(privateBytes)
	if err != nil {
		logger.Fatal(fmt.Errorf("invalid arguments passed, private key file is not parseable: %v", err), "unable to start SFTP server")
	}

	// An SSH server is represented by a ServerConfig, which holds
	// certificate details and handles authentication of ServerConns.
	sshConfig := &ssh.ServerConfig{
		Config: ssh.Config{
			KeyExchanges: allowKexAlgos,
			Ciphers:      allowCiphers,
			MACs:         allowMACs,
		},
		PublicKeyAuthAlgorithms: allowPubKeys,
		PasswordCallback: func(c ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if globalIAMSys.LDAPConfig.Enabled() {
				sa, _, err := globalIAMSys.getServiceAccount(context.Background(), c.User())
				if err != nil && !errors.Is(err, errNoSuchServiceAccount) {
					return nil, err
				}
				if errors.Is(err, errNoSuchServiceAccount) {
					targetUser, targetGroups, err := globalIAMSys.LDAPConfig.Bind(c.User(), string(pass))
					if err != nil {
						return nil, err
					}
					ldapPolicies, _ := globalIAMSys.PolicyDBGet(targetUser, targetGroups...)
					if len(ldapPolicies) == 0 {
						return nil, errAuthentication
					}
					return &ssh.Permissions{
						CriticalOptions: map[string]string{
							ldapUser:  targetUser,
							ldapUserN: c.User(),
						},
						Extensions: make(map[string]string),
					}, nil
				}
				if subtle.ConstantTimeCompare([]byte(sa.Credentials.SecretKey), pass) == 1 {
					return &ssh.Permissions{
						CriticalOptions: map[string]string{
							"accessKey": c.User(),
						},
						Extensions: make(map[string]string),
					}, nil
				}
				return nil, errAuthentication
			}

			ui, ok := globalIAMSys.GetUser(context.Background(), c.User())
			if !ok {
				return nil, errNoSuchUser
			}

			if subtle.ConstantTimeCompare([]byte(ui.Credentials.SecretKey), pass) == 1 {
				return &ssh.Permissions{
					CriticalOptions: map[string]string{
						"accessKey": c.User(),
					},
					Extensions: make(map[string]string),
				}, nil
			}
			return nil, errAuthentication
		},
	}

	if userCaKeyFile != "" {
		keyBytes, err := os.ReadFile(userCaKeyFile)
		if err != nil {
			logger.Fatal(fmt.Errorf("invalid arguments passed, trusted user certificate authority public key file is not accessible: %v", err), "unable to start SFTP server")
		}

		caPublicKey, _, _, _, err := ssh.ParseAuthorizedKey(keyBytes)
		if err != nil {
			logger.Fatal(fmt.Errorf("invalid arguments passed, trusted user certificate authority public key file is not parseable: %v", err), "unable to start SFTP server")
		}

		sshConfig.PublicKeyCallback = func(c ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
			_, ok := globalIAMSys.GetUser(context.Background(), c.User())
			if !ok {
				return nil, errNoSuchUser
			}

			// Verify that client provided certificate, not only public key.
			cert, ok := key.(*ssh.Certificate)
			if !ok {
				return nil, errSftpPublicKeyWithoutCert
			}

			// ssh.CheckCert called by ssh.Authenticate accepts certificates
			// with empty principles list so we block those in here.
			if len(cert.ValidPrincipals) == 0 {
				return nil, errSftpCertWithoutPrincipals
			}

			// Verify that certificate provided by user is issued by trusted CA,
			// username in authentication request matches to identities in certificate
			// and that certificate type is correct.
			checker := ssh.CertChecker{}
			checker.IsUserAuthority = func(k ssh.PublicKey) bool {
				return bytes.Equal(k.Marshal(), caPublicKey.Marshal())
			}
			_, err = checker.Authenticate(c, key)
			if err != nil {
				return nil, err
			}

			return &ssh.Permissions{
				CriticalOptions: map[string]string{
					"accessKey": c.User(),
				},
				Extensions: make(map[string]string),
			}, nil
		}
	}

	sshConfig.AddHostKey(private)

	handleSFTPSession := func(channel ssh.Channel, sconn *ssh.ServerConn) {
		server := sftp.NewRequestServer(channel, NewSFTPDriver(sconn.Permissions), sftp.WithRSAllocator())
		defer server.Close()
		server.Serve()
	}

	sftpServer, err := xsftp.NewServer(&xsftp.Options{
		PublicIP: publicIP,
		Port:     port,
		// OpensSSH default handshake timeout is 2 minutes.
		SSHHandshakeDeadline: 2 * time.Minute,
		Logger:               new(sftpLogger),
		SSHConfig:            sshConfig,
		HandleSFTPSession:    handleSFTPSession,
	})
	if err != nil {
		logger.Fatal(err, "Unable to start SFTP Server")
	}

	err = sftpServer.Listen()
	if err != nil {
		logger.Fatal(err, "SFTP Server had an unrecoverable error while accepting connections")
	}
}

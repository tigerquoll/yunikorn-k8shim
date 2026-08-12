/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

package main

import (
	"crypto/tls"
	"crypto/x509"
	"sync"
	"testing"
	"time"

	"gotest.tools/v3/assert"

	"github.com/apache/yunikorn-k8shim/pkg/admission/pki"
)

// testCertificate builds a self signed server certificate for the test server. The webhook needs a
// usable certificate to start listening, nothing in these tests completes a handshake.
func testCertificate(t *testing.T) *tls.Certificate {
	t.Helper()
	caCert, caKey, err := pki.GenerateCACertificate(time.Now().AddDate(1, 0, 0))
	assert.NilError(t, err, "failed to generate the CA certificate")
	cert, key, err := pki.GenerateServerCertificate("localhost", []string{"localhost"}, caCert, caKey)
	assert.NilError(t, err, "failed to generate the server certificate")
	certPem, err := pki.EncodeCertChainPem([]*x509.Certificate{cert, caCert})
	assert.NilError(t, err, "failed to encode the certificate chain")
	keyPem, err := pki.EncodePrivateKeyPem(key)
	assert.NilError(t, err, "failed to encode the private key")
	pair, err := tls.X509KeyPair(*certPem, *keyPem)
	assert.NilError(t, err, "failed to build the key pair")
	return &pair
}

// newTestWebhook returns a webhook on a free port. The admission controller is not needed: the
// handlers are only bound to the mux, no request is served.
func newTestWebhook() *WebHook {
	return CreateWebhook(nil, 0)
}

// TestWebHookShutdownDuringStartup runs a shutdown against a startup that has just returned. The
// server the serving routine uses must be the one that startup created, not whatever the field
// holds by the time the routine runs: a shutdown in between clears the field, and dereferencing it
// there panics the admission controller. Run with -race, the field is written under the lock and
// the routine used to read it without.
func TestWebHookShutdownDuringStartup(t *testing.T) {
	certs := testCertificate(t)

	for i := 0; i < 50; i++ {
		webhook := newTestWebhook()
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			webhook.Startup(certs)
		}()
		go func() {
			defer wg.Done()
			webhook.Shutdown()
		}()
		wg.Wait()

		// whichever order the two ran in, a shutdown afterwards must leave nothing behind
		webhook.Shutdown()
		webhook.Lock()
		assert.Assert(t, webhook.server == nil, "the server must be cleared by shutdown")
		webhook.Unlock()
	}
}

// TestWebHookStartupShutdownCycle covers the sequence the controller itself runs when it reloads
// the certificates: shutdown then startup, repeatedly, on one webhook.
func TestWebHookStartupShutdownCycle(t *testing.T) {
	certs := testCertificate(t)
	webhook := newTestWebhook()

	for i := 0; i < 10; i++ {
		webhook.Startup(certs)
		webhook.Lock()
		assert.Assert(t, webhook.server != nil, "startup must leave a server behind")
		webhook.Unlock()
		webhook.Shutdown()
		webhook.Lock()
		assert.Assert(t, webhook.server == nil, "shutdown must clear the server")
		webhook.Unlock()
	}
}

// TestWebHookShutdownWithoutStartup shuts down a webhook that was never started.
func TestWebHookShutdownWithoutStartup(t *testing.T) {
	webhook := newTestWebhook()
	webhook.Shutdown()
	webhook.Lock()
	assert.Assert(t, webhook.server == nil, "there is nothing to shut down")
	webhook.Unlock()
}

// TestWebHookConcurrentShutdown runs several shutdowns at once against a started webhook: only one
// of them owns the server, the others must return without touching it.
func TestWebHookConcurrentShutdown(t *testing.T) {
	certs := testCertificate(t)
	webhook := newTestWebhook()
	webhook.Startup(certs)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			webhook.Shutdown()
		}()
	}
	wg.Wait()

	webhook.Lock()
	assert.Assert(t, webhook.server == nil, "the server must be cleared by shutdown")
	webhook.Unlock()
}

package broker

import (
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Josepavese/videosluice/internal/unixsocket"
)

func TestAttachClientRejectsRetiredPublicationBeforeMutatingRuntime(t *testing.T) {
	for _, role := range []byte{brokerStreamH264, brokerEndpointArchive, brokerStreamNV12} {
		for _, boundary := range []string{"removed", "disabled", "replaced", "lost_inode", "canceled"} {
			name := map[byte]string{brokerStreamH264: "live", brokerEndpointArchive: "archive", brokerStreamNV12: "analysis"}[role]
			t.Run(name+"/"+boundary, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "stream.sock")
				publication, err := listenBrokerSocket(path)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(publication.Cleanup)
				runtime := &videoBrokerRuntime{
					plan: videoBrokerPlan{
						LiveEnabled: true, ArchiveEnabled: true, AnalysisEnabled: true,
						MainSocketPath: path, ArchiveSocketPath: path, SubSocketPath: path,
						MainWriteTimeoutMs: 10,
					},
					mainClients: map[int]*brokerClient{}, archiveClients: map[int]*brokerClient{},
					subClients: map[int]*brokerClient{}, childRunning: true,
				}
				bind := func(p *unixsocket.Publication) {
					switch role {
					case brokerStreamH264:
						runtime.mainPublication = p
					case brokerEndpointArchive:
						runtime.archivePublication = p
					case brokerStreamNV12:
						runtime.subPublication = p
					}
				}
				bind(publication)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				switch boundary {
				case "removed":
					bind(nil)
				case "disabled":
					runtime.plan.LiveEnabled, runtime.plan.ArchiveEnabled, runtime.plan.AnalysisEnabled = false, false, false
				case "replaced", "lost_inode":
					replacement, err := listenBrokerSocket(filepath.Join(filepath.Dir(path), "candidate.sock"))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(replacement.Cleanup)
					if boundary == "replaced" {
						bind(replacement)
					} else if err := replacement.Replace(publication); err != nil {
						t.Fatal(err)
					}
				case "canceled":
					cancel()
				}
				server, client := net.Pipe()
				t.Cleanup(func() {
					_ = server.Close()
					_ = client.Close()
					done := make(chan struct{})
					go func() { runtime.wg.Wait(); close(done) }()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Error("rejected attachment retained a client goroutine")
					}
				})
				// Models Accept returning on the superseded listener just as a
				// cutover releases runtime.mu: no old client may be registered.
				if err := runtime.attachClient(ctx, publication, role, server); err == nil {
					t.Fatal("superseded publication admitted a client")
				}
				if runtime.nextClientID != 0 || len(runtime.mainClients)+len(runtime.archiveClients)+len(runtime.subClients) != 0 || !runtime.mainDemandedAt.IsZero() {
					t.Fatal("rejected publication changed demand or client ownership")
				}
			})
		}
	}
}

// startGoroutine refuses to start anything once the runtime is stopping, and
// attachClient used to ignore that refusal: the client was already registered,
// so the connection stayed open, attached and unserved until the process
// exited — no write loop, no watcher, and acceptLoop only closes a connection
// when attachClient reports a failure.
func TestAttachClientReleasesTheConnectionWhenTheRuntimeIsStopping(t *testing.T) {
	for _, role := range []byte{brokerStreamH264, brokerEndpointArchive, brokerStreamNV12} {
		name := map[byte]string{brokerStreamH264: "live", brokerEndpointArchive: "archive", brokerStreamNV12: "analysis"}[role]
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "stream.sock")
			publication, err := listenBrokerSocket(path)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(publication.Cleanup)
			runtime := &videoBrokerRuntime{
				plan: videoBrokerPlan{
					LiveEnabled: true, ArchiveEnabled: true, AnalysisEnabled: true,
					MainSocketPath: path, ArchiveSocketPath: path, SubSocketPath: path,
					MainWriteTimeoutMs: 10,
				},
				mainClients: map[int]*brokerClient{}, archiveClients: map[int]*brokerClient{},
				subClients: map[int]*brokerClient{}, childRunning: true,
				stopping: true,
			}
			switch role {
			case brokerStreamH264:
				runtime.mainPublication = publication
			case brokerEndpointArchive:
				runtime.archivePublication = publication
			case brokerStreamNV12:
				runtime.subPublication = publication
			}
			server, client := net.Pipe()
			t.Cleanup(func() {
				_ = server.Close()
				_ = client.Close()
			})

			err = runtime.attachClient(context.Background(), publication, role, server)
			if err == nil {
				t.Fatal("uno shutdown in corso ha ammesso un client che non puo' essere servito")
			}
			if !strings.Contains(err.Error(), "stopping") {
				t.Fatalf("attach fallito per un motivo inatteso: %v", err)
			}
			if registered := len(runtime.mainClients) + len(runtime.archiveClients) + len(runtime.subClients); registered != 0 {
				t.Fatalf("client rimasto agganciato senza essere servito: %d", registered)
			}
			// La connessione deve essere rilasciata: acceptLoop la chiude solo
			// quando attachClient riporta un errore.
			_ = client.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := client.Read(make([]byte, 1)); err == nil {
				t.Fatal("la connessione e' rimasta aperta e non servita")
			}
		})
	}
}

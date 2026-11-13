// Command dragonfly-transport provides the Bedrock transport edge for the Rust MC-RS
// runtime. It owns RakNet, Bedrock login authentication, encryption and compression;
// gameplay packets are forwarded without interpretation over one private loopback TCP stream per player.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/sandertv/gophertunnel/minecraft"
	"github.com/sandertv/gophertunnel/minecraft/protocol/packet"
)

const (
	maxIPCFrame = 32 << 20
	dialTimeout = 10 * time.Second
	expectedBedrockProtocol int32 = 2225
)

type sessionHello struct {
	Type          string `json:"type"`
	Protocol      int32  `json:"protocol"`
	Username      string `json:"username"`
	UUID          string `json:"uuid"`
	XUID          string `json:"xuid,omitempty"`
	RemoteAddress string          `json:"remote_address"`
	ClientData    json.RawMessage `json:"client_data,omitempty"`
}

func main() {
	bind := flag.String("bind", "0.0.0.0:19132", "Bedrock RakNet listen address")
	ipc := flag.String("ipc", "127.0.0.1:19133", "Rust loopback TCP transport endpoint")
	authDisabled := flag.Bool("auth-disabled", false, "disable Xbox Live authentication (development only)")
	maxPlayers := flag.Int("max-players", 20, "maximum concurrent Bedrock sessions")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if got := minecraft.DefaultProtocol.ID(); got != expectedBedrockProtocol {
		logger.Error("unexpected Gophertunnel protocol", "got", got, "want", expectedBedrockProtocol)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := minecraft.ListenConfig{
		AuthenticationDisabled: *authDisabled,
		MaximumPlayers:          *maxPlayers,
		LoginTimeout:            15 * time.Second,
		AllowUnknownPackets:     true,
		AllowInvalidPackets:     true,
		FlushRate:               50 * time.Millisecond,
		// Gophertunnel consumes resource-pack negotiation internally. Notify
		// MC-RS when the client confirms the empty pack stack, so Rust can
		// begin its original PreSpawn/StartGame sequence without duplicating
		// LoginSuccess or ResourcePacksInfo.
		PacketFunc: func(header packet.Header, payload []byte, src, dst net.Addr) {
			if header.PacketID != packet.IDResourcePackClientResponse || src == nil || !isResourcePackStackFinished(payload) {
				return
			}
			queue := sessionSignalQueue(src.String())
			raw := encodePacket(header, payload)
			select {
			case queue <- raw:
			default:
				logger.Warn("resource-pack completion signal queue is full", "remote", src)
			}
		},
	}
	listener, err := cfg.Listen("raknet", *bind)
	if err != nil {
		logger.Error("start Bedrock listener", "error", err)
		os.Exit(1)
	}
	defer listener.Close()
	logger.Info("Dragonfly/Gophertunnel transport listening", "address", listener.Addr(), "protocol", minecraft.DefaultProtocol.ID(), "ipc", *ipc)

	var sessions sync.WaitGroup
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	for {
		rawConn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			logger.Warn("accept Bedrock connection", "error", err)
			continue
		}
		conn, ok := rawConn.(*minecraft.Conn)
		if !ok {
			_ = rawConn.Close()
			logger.Warn("listener returned an unexpected connection type")
			continue
		}
		sessions.Add(1)
		go func() {
			defer sessions.Done()
			if err := serveSession(ctx, conn, *ipc); err != nil && !errors.Is(err, net.ErrClosed) && ctx.Err() == nil {
				logger.Info("Bedrock session ended", "remote", conn.RemoteAddr(), "error", err)
			}
		}()
	}
	sessions.Wait()
	logger.Info("transport stopped")
}

var sessionSignals sync.Map // remote address -> buffered resource-pack completion events

func sessionSignalQueue(remote string) chan []byte {
	value, _ := sessionSignals.LoadOrStore(remote, make(chan []byte, 16))
	return value.(chan []byte)
}

func isResourcePackStackFinished(payload []byte) bool {
	reader := bytes.NewReader(payload)
	response, err := binary.ReadUvarint(reader)
	return err == nil && response == packet.PackResponseCompleted
}

// encodePacket reconstructs a complete Bedrock packet header and payload.
func encodePacket(header packet.Header, payload []byte) []byte {
	id := header.PacketID | (uint32(header.SenderSubClient) << 10) | (uint32(header.TargetSubClient) << 12)
	var encoded [10]byte
	n := binary.PutUvarint(encoded[:], uint64(id))
	raw := make([]byte, n+len(payload))
	copy(raw, encoded[:n])
	copy(raw[n:], payload)
	return raw
}

func serveSession(ctx context.Context, conn *minecraft.Conn, socketPath string) error {
	defer conn.Close()
	remoteKey := conn.RemoteAddr().String()
	controlPackets := sessionSignalQueue(remoteKey)
	defer sessionSignals.CompareAndDelete(remoteKey, controlPackets)
	ipc, err := dialRust(ctx, socketPath)
	if err != nil {
		return fmt.Errorf("connect Rust runtime: %w", err)
	}
	defer ipc.Close()
	sessionDone := make(chan struct{})
	defer close(sessionDone)

	identity := conn.IdentityData()
	clientData, err := json.Marshal(conn.ClientData())
	if err != nil {
		return fmt.Errorf("encode client data: %w", err)
	}
	hello, err := json.Marshal(sessionHello{
		Type:          "open",
		Protocol:      conn.Proto().ID(),
		Username:      identity.DisplayName,
		UUID:          identity.Identity,
		XUID:          identity.XUID,
		RemoteAddress: conn.RemoteAddr().String(),
		ClientData:    clientData,
	})
	if err != nil {
		return fmt.Errorf("encode session metadata: %w", err)
	}
	if err := writeFrame(ipc, hello); err != nil {
		return fmt.Errorf("send session metadata: %w", err)
	}

	var ipcWriteMu sync.Mutex
	writeIPC := func(raw []byte) error {
		ipcWriteMu.Lock()
		defer ipcWriteMu.Unlock()
		return writeFrame(ipc, raw)
	}

	readErr := make(chan error, 1)
	go func() {
		defer close(readErr)
		for {
			raw, err := conn.ReadBytes()
			if err != nil {
				readErr <- err
				_ = ipc.Close()
				return
			}
			if err := writeIPC(raw); err != nil {
				readErr <- fmt.Errorf("forward client packet: %w", err)
				_ = ipc.Close()
				return
			}
		}
	}()
	go func() {
		for {
			select {
			case raw := <-controlPackets:
				if err := writeIPC(raw); err != nil {
					readErr <- fmt.Errorf("forward resource-pack completion: %w", err)
					_ = ipc.Close()
					return
				}
			case <-sessionDone:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		select {
		case <-ctx.Done():
			_ = ipc.Close()
		case <-sessionDone:
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-readErr:
			return err
		default:
		}

		raw, err := readFrame(ipc)
		if err != nil {
			return fmt.Errorf("read Rust packet: %w", err)
		}
		if err := writeBedrockPacket(conn, raw); err != nil {
			return fmt.Errorf("write Bedrock packet: %w", err)
		}
	}
}

func dialRust(ctx context.Context, path string) (net.Conn, error) {
	deadline := time.Now().Add(dialTimeout)
	var last error
	for {
		conn, err := (&net.Dialer{Timeout: 500 * time.Millisecond}).DialContext(ctx, "tcp", path)
		if err == nil {
			return conn, nil
		}
		last = err
		if time.Now().After(deadline) {
			return nil, last
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// writeBedrockPacket accepts one packet encoded as VarUInt32 packet ID followed by
// its uncompressed payload. Gophertunnel then applies protocol framing, compression
// and encryption for the already-authenticated client session.
func writeBedrockPacket(conn *minecraft.Conn, raw []byte) error {
	reader := bytes.NewBuffer(raw)
	var header packet.Header
	if err := header.Read(reader); err != nil {
		return fmt.Errorf("decode packet header: %w", err)
	}
	if header.SenderSubClient != 0 || header.TargetSubClient != 0 {
		return fmt.Errorf("split-screen packet routing is unsupported (sender=%d target=%d)", header.SenderSubClient, header.TargetSubClient)
	}
	if err := conn.WritePacket(&packet.Unknown{PacketID: header.PacketID, Payload: reader.Bytes()}); err != nil {
		return err
	}
	return conn.Flush()
}

func writeFrame(w io.Writer, payload []byte) error {
	if len(payload) == 0 || len(payload) > maxIPCFrame {
		return fmt.Errorf("invalid IPC frame size %d", len(payload))
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, payload)
}

func writeAll(w io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := w.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func readFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > maxIPCFrame {
		return nil, fmt.Errorf("invalid IPC frame size %d", size)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

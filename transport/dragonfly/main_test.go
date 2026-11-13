package main

import (
	"bytes"

	"github.com/sandertv/gophertunnel/minecraft"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	want := []byte{0x01, 0x7f, 0x00, 0xff}
	var stream bytes.Buffer
	if err := writeFrame(&stream, want); err != nil {
		t.Fatal(err)
	}
	got, err := readFrame(&stream)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("round trip mismatch: got %x, want %x", got, want)
	}
}

func TestFrameRejectsEmptyAndOversizedPayload(t *testing.T) {
	var stream bytes.Buffer
	if err := writeFrame(&stream, nil); err == nil {
		t.Fatal("expected empty frame to fail")
	}
	var header [4]byte
	header[0], header[1], header[2], header[3] = 0x02, 0x00, 0x00, 0x01
	if _, err := readFrame(bytes.NewReader(header[:])); err == nil {
		t.Fatal("expected oversized frame to fail")
	}
}

func TestGophertunnelProtocolIsBedrock12660(t *testing.T) {
	if got := minecraft.DefaultProtocol.ID(); got != expectedBedrockProtocol {
		t.Fatalf("Gophertunnel protocol = %d, want %d (Bedrock 1.26.60)", got, expectedBedrockProtocol)
	}
}

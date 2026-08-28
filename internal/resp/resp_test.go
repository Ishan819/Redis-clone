package resp

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestMarshal(t *testing.T) {
	tests := []struct {
		name string
		in   Value
		want string
	}{
		{"simple string", SimpleStringValue("OK"), "+OK\r\n"},
		{"error", ErrorValue("ERR bad command"), "-ERR bad command\r\n"},
		{"integer", IntegerValue(1000), ":1000\r\n"},
		{"negative integer", IntegerValue(-1), ":-1\r\n"},
		{"bulk string", BulkStringValue("hello"), "$5\r\nhello\r\n"},
		{"empty bulk string", BulkStringValue(""), "$0\r\n\r\n"},
		{"null bulk string", NullBulkString(), "$-1\r\n"},
		{"null array", NullArray(), "*-1\r\n"},
		{"empty array", ArrayValue(), "*0\r\n"},
		{
			"array of bulk strings",
			ArrayValue(BulkStringValue("ECHO"), BulkStringValue("hi")),
			"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n",
		},
		{
			"nested array",
			ArrayValue(ArrayValue(IntegerValue(1), IntegerValue(2)), SimpleStringValue("OK")),
			"*2\r\n*2\r\n:1\r\n:2\r\n+OK\r\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(tt.in.Marshal())
			if got != tt.want {
				t.Errorf("Marshal() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReaderRead(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want Value
	}{
		{"simple string", "+OK\r\n", SimpleStringValue("OK")},
		{"error", "-ERR bad command\r\n", ErrorValue("ERR bad command")},
		{"integer", ":1000\r\n", IntegerValue(1000)},
		{"negative integer", ":-1\r\n", IntegerValue(-1)},
		{"bulk string", "$5\r\nhello\r\n", BulkStringValue("hello")},
		{"empty bulk string", "$0\r\n\r\n", BulkStringValue("")},
		{"null bulk string", "$-1\r\n", NullBulkString()},
		{"null array", "*-1\r\n", NullArray()},
		{"empty array", "*0\r\n", ArrayValue()},
		{
			"array of bulk strings",
			"*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n",
			ArrayValue(BulkStringValue("ECHO"), BulkStringValue("hi")),
		},
		{
			"nested array",
			"*2\r\n*2\r\n:1\r\n:2\r\n+OK\r\n",
			ArrayValue(ArrayValue(IntegerValue(1), IntegerValue(2)), SimpleStringValue("OK")),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.in))
			got, err := r.Read()
			if err != nil {
				t.Fatalf("Read() error = %v", err)
			}
			if !valuesEqual(got, tt.want) {
				t.Errorf("Read() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestRoundTrip checks that Marshal followed by Read reproduces the
// original value, for every case in the marshal table.
func TestRoundTrip(t *testing.T) {
	values := []Value{
		SimpleStringValue("OK"),
		ErrorValue("ERR bad command"),
		IntegerValue(1000),
		IntegerValue(-1),
		BulkStringValue("hello"),
		BulkStringValue(""),
		NullBulkString(),
		NullArray(),
		ArrayValue(),
		ArrayValue(BulkStringValue("ECHO"), BulkStringValue("hi")),
	}

	for _, v := range values {
		encoded := v.Marshal()
		r := NewReader(strings.NewReader(string(encoded)))
		got, err := r.Read()
		if err != nil {
			t.Fatalf("Read() after Marshal(%+v) error = %v", v, err)
		}
		if !valuesEqual(got, v) {
			t.Errorf("round trip: got %+v, want %+v", got, v)
		}
	}
}

// TestReadMultipleCommands checks that consecutive commands on the same
// stream (as a real connection would send) are each decoded correctly,
// with the reader stopping at io.EOF once the stream is exhausted.
func TestReadMultipleCommands(t *testing.T) {
	in := "*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"
	r := NewReader(strings.NewReader(in))

	first, err := r.Read()
	if err != nil {
		t.Fatalf("first Read() error = %v", err)
	}
	if !valuesEqual(first, ArrayValue(BulkStringValue("PING"))) {
		t.Errorf("first Read() = %+v", first)
	}

	second, err := r.Read()
	if err != nil {
		t.Fatalf("second Read() error = %v", err)
	}
	if !valuesEqual(second, ArrayValue(BulkStringValue("ECHO"), BulkStringValue("hi"))) {
		t.Errorf("second Read() = %+v", second)
	}

	if _, err := r.Read(); !errors.Is(err, io.EOF) {
		t.Errorf("third Read() error = %v, want io.EOF", err)
	}
}

// TestBuffered exercises Buffered() the way internal/eventloop's epoll
// implementation actually uses it: decode one Value out of a byte slice
// that holds more than one pipelined command, and use
// len(slice)-Buffered() to find exactly how many bytes that first Value
// consumed, so the remaining bytes can be re-parsed as the next command.
func TestBuffered(t *testing.T) {
	first := "*1\r\n$4\r\nPING\r\n"
	second := "*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\n"
	data := []byte(first + second)

	r := NewReader(bytes.NewReader(data))
	val, err := r.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if !valuesEqual(val, ArrayValue(BulkStringValue("PING"))) {
		t.Fatalf("Read() = %+v", val)
	}

	consumed := len(data) - r.Buffered()
	if consumed != len(first) {
		t.Fatalf("consumed = %d, want %d (len of first command)", consumed, len(first))
	}

	// The unconsumed remainder should decode as the second command on a
	// fresh Reader, exactly as the event loop re-parses it next.
	r2 := NewReader(bytes.NewReader(data[consumed:]))
	val2, err := r2.Read()
	if err != nil {
		t.Fatalf("re-Read() error = %v", err)
	}
	if !valuesEqual(val2, ArrayValue(BulkStringValue("ECHO"), BulkStringValue("hi"))) {
		t.Errorf("re-Read() = %+v", val2)
	}
}

// TestBufferedIncompleteValue confirms that when a Value is only partially
// present, Read fails with an error wrapping io.EOF or io.ErrUnexpectedEOF
// (never any other error) — this is exactly the signal
// internal/eventloop's epoll implementation relies on to tell "wait for
// more bytes" apart from a genuine protocol error.
func TestBufferedIncompleteValue(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"truncated type line", "*2\r\n$4\r\nECHO"},
		{"truncated bulk string body", "*1\r\n$5\r\nhel"},
		{"array missing an element", "*2\r\n$4\r\nECHO\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(bytes.NewReader([]byte(tt.in)))
			_, err := r.Read()
			if err == nil {
				t.Fatalf("Read() succeeded on incomplete input %q", tt.in)
			}
			if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Errorf("Read() error = %v, want it to wrap io.EOF or io.ErrUnexpectedEOF", err)
			}
		})
	}
}

func TestReadErrors(t *testing.T) {
	tests := []struct {
		name string
		in   string
	}{
		{"unknown type byte", "@nope\r\n"},
		{"bad integer", ":not-a-number\r\n"},
		{"bad bulk string length", "$abc\r\nhello\r\n"},
		{"bad array length", "*abc\r\n"},
		{"truncated bulk string", "$5\r\nhi\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.in))
			if _, err := r.Read(); err == nil {
				t.Errorf("Read(%q) succeeded, want error", tt.in)
			}
		})
	}
}

// valuesEqual compares two Values for the purposes of these tests
// (reflect.DeepEqual works too, but this gives clearer failure messages
// when a single field is off).
func valuesEqual(a, b Value) bool {
	if a.Type != b.Type || a.Str != b.Str || a.Num != b.Num || a.Null != b.Null {
		return false
	}
	if len(a.Array) != len(b.Array) {
		return false
	}
	for i := range a.Array {
		if !valuesEqual(a.Array[i], b.Array[i]) {
			return false
		}
	}
	return true
}

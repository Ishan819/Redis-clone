// Package resp implements encoding and decoding of the Redis Serialization
// Protocol (RESP), the wire format Redis clients (including redis-cli) use
// to talk to a Redis server.
//
// RESP messages are self-delimiting: the first byte of a message identifies
// its type, and the rest of the message tells the reader exactly how many
// more bytes to consume. This package covers the five basic RESP2 types:
// simple strings, errors, integers, bulk strings, and arrays.
package resp

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

// Type identifies which of the five RESP data types a Value holds. It is
// also the leading byte of the value's wire representation.
type Type byte

const (
	SimpleString Type = '+'
	Error        Type = '-'
	Integer      Type = ':'
	BulkString   Type = '$'
	Array        Type = '*'
)

// terminator ends every RESP line.
const terminator = "\r\n"

// Value is a single RESP message. Only the fields relevant to Type are
// populated:
//
//   - SimpleString, Error, BulkString store their payload in Str.
//   - Integer stores its payload in Num.
//   - Array stores its elements in Array.
//   - BulkString and Array additionally support a "null" form (Redis's
//     $-1\r\n and *-1\r\n), signaled by Null.
type Value struct {
	Type  Type
	Str   string
	Num   int64
	Array []Value
	Null  bool
}

// Constructors for the values commands most commonly need to send back.

// SimpleStringValue builds a RESP simple string, e.g. "+OK\r\n". Simple
// strings must not contain \r or \n; use BulkStringValue for arbitrary data.
func SimpleStringValue(s string) Value {
	return Value{Type: SimpleString, Str: s}
}

// ErrorValue builds a RESP error reply from a plain message, e.g.
// "-ERR unknown command\r\n". By Redis convention the message starts with
// an upper-case error code (ERR, WRONGTYPE, ...) followed by a space.
func ErrorValue(msg string) Value {
	return Value{Type: Error, Str: msg}
}

// Errorf is ErrorValue with fmt.Sprintf-style formatting.
func Errorf(format string, a ...any) Value {
	return Value{Type: Error, Str: fmt.Sprintf(format, a...)}
}

// IntegerValue builds a RESP integer, e.g. ":1000\r\n".
func IntegerValue(n int64) Value {
	return Value{Type: Integer, Num: n}
}

// BulkStringValue builds a RESP bulk string, which can hold arbitrary
// binary-safe data.
func BulkStringValue(s string) Value {
	return Value{Type: BulkString, Str: s}
}

// NullBulkString is the RESP "no value" reply, $-1\r\n. Redis returns this
// for, e.g., GET on a missing key.
func NullBulkString() Value {
	return Value{Type: BulkString, Null: true}
}

// ArrayValue builds a RESP array from its elements.
func ArrayValue(vs ...Value) Value {
	return Value{Type: Array, Array: vs}
}

// NullArray is the RESP "no array" reply, *-1\r\n.
func NullArray() Value {
	return Value{Type: Array, Null: true}
}

// Marshal encodes v to its RESP wire representation.
func (v Value) Marshal() []byte {
	var buf bytes.Buffer
	v.write(&buf)
	return buf.Bytes()
}

func (v Value) write(buf *bytes.Buffer) {
	switch v.Type {
	case SimpleString:
		buf.WriteByte(byte(SimpleString))
		buf.WriteString(v.Str)
		buf.WriteString(terminator)
	case Error:
		buf.WriteByte(byte(Error))
		buf.WriteString(v.Str)
		buf.WriteString(terminator)
	case Integer:
		buf.WriteByte(byte(Integer))
		buf.WriteString(strconv.FormatInt(v.Num, 10))
		buf.WriteString(terminator)
	case BulkString:
		buf.WriteByte(byte(BulkString))
		if v.Null {
			buf.WriteString("-1")
			buf.WriteString(terminator)
			return
		}
		buf.WriteString(strconv.Itoa(len(v.Str)))
		buf.WriteString(terminator)
		buf.WriteString(v.Str)
		buf.WriteString(terminator)
	case Array:
		buf.WriteByte(byte(Array))
		if v.Null {
			buf.WriteString("-1")
			buf.WriteString(terminator)
			return
		}
		buf.WriteString(strconv.Itoa(len(v.Array)))
		buf.WriteString(terminator)
		for _, elem := range v.Array {
			elem.write(buf)
		}
	default:
		// Marshal is only ever called on Values this package constructs, so
		// an unknown type means a bug in this package, not bad input.
		panic(fmt.Sprintf("resp: cannot marshal unknown type %q", byte(v.Type)))
	}
}

// Reader decodes a stream of RESP values, such as the sequence of commands
// a client sends over a single connection.
type Reader struct {
	br *bufio.Reader
}

// NewReader wraps r for RESP decoding.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReader(r)}
}

// Buffered returns the number of bytes currently held in the Reader's
// internal buffer that have been read from the underlying io.Reader but not
// yet consumed by Read. Callers that feed Read a byte slice wrapped in a
// bytes.Reader (rather than a live connection) can use
// len(slice)-Buffered() after a successful Read to find out exactly how
// many bytes of the slice that one Value consumed — e.g. an epoll-driven
// event loop that accumulates raw bytes per connection and needs to know
// how far to advance its own buffer after decoding one pipelined command.
func (r *Reader) Buffered() int {
	return r.br.Buffered()
}

// Read decodes and returns the next RESP value from the stream. It returns
// io.EOF (unwrapped, so errors.Is(err, io.EOF) succeeds) when the stream
// ends cleanly between values.
func (r *Reader) Read() (Value, error) {
	line, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, fmt.Errorf("resp: empty line where a type byte was expected")
	}

	typ := Type(line[0])
	body := line[1:]

	switch typ {
	case SimpleString:
		return Value{Type: SimpleString, Str: body}, nil
	case Error:
		return Value{Type: Error, Str: body}, nil
	case Integer:
		n, err := strconv.ParseInt(body, 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("resp: invalid integer %q: %w", body, err)
		}
		return Value{Type: Integer, Num: n}, nil
	case BulkString:
		return r.readBulkString(body)
	case Array:
		return r.readArray(body)
	default:
		return Value{}, fmt.Errorf("resp: unknown type byte %q", line[0])
	}
}

func (r *Reader) readBulkString(lengthField string) (Value, error) {
	n, err := strconv.Atoi(lengthField)
	if err != nil {
		return Value{}, fmt.Errorf("resp: invalid bulk string length %q: %w", lengthField, err)
	}
	if n < 0 {
		// The only negative length Redis sends is -1, meaning a null bulk
		// string; treat any other negative length as an alias for it.
		return Value{Type: BulkString, Null: true}, nil
	}

	data := make([]byte, n+len(terminator))
	if _, err := io.ReadFull(r.br, data); err != nil {
		return Value{}, fmt.Errorf("resp: reading bulk string body: %w", err)
	}
	return Value{Type: BulkString, Str: string(data[:n])}, nil
}

func (r *Reader) readArray(countField string) (Value, error) {
	n, err := strconv.Atoi(countField)
	if err != nil {
		return Value{}, fmt.Errorf("resp: invalid array length %q: %w", countField, err)
	}
	if n < 0 {
		return Value{Type: Array, Null: true}, nil
	}

	elems := make([]Value, n)
	for i := range elems {
		elem, err := r.Read()
		if err != nil {
			return Value{}, fmt.Errorf("resp: reading array element %d: %w", i, err)
		}
		elems[i] = elem
	}
	return Value{Type: Array, Array: elems}, nil
}

// readLine reads up to and including the next "\r\n", returning the line
// without the trailing terminator.
func (r *Reader) readLine() (string, error) {
	line, err := r.br.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = line[:len(line)-1] // trim '\n'
	line = trimCR(line)
	return line, nil
}

func trimCR(s string) string {
	if len(s) > 0 && s[len(s)-1] == '\r' {
		return s[:len(s)-1]
	}
	return s
}

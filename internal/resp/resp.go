package resp

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const (
	STRING  = '+'
	ERROR   = '-'
	INTEGER = ':'
	BULK    = '$'
	ARRAY   = '*'
)

type Value struct {
	Typ   string
	Str   string
	Num   string
	Bulk  string
	Array []Value
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(rd io.Reader) *Resp {
	return &Resp{reader: bufio.NewReader(rd)}
}

func (r *Resp) Read() (Value, error) {

	char, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}

	switch char {
	case ARRAY:
		return r.ReadArray()
	case BULK:
		return r.ReadBulk()
	default:
		fmt.Printf("Unknown Type: %v", string(char))
		return Value{}, nil
	}
}

func (r *Resp) ReadLine() (line []byte, n int, err error) {

	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n++
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == '\r' {
			break
		}
	}

	return line[:len(line)-2], n, nil
}

func (r *Resp) ReadInteger() (val int, n int, err error) {

	line, n, err := r.ReadLine()
	if err != nil {
		return 0, 0, err
	}

	v, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}

	return int(v), n, nil
}

func (r *Resp) ReadArray() (Value, error) {

	v := Value{}
	v.Typ = "Array"

	len, _, err := r.ReadInteger()
	if err != nil {
		return v, err
	}

	v.Array = make([]Value, 0)

	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return v, err
		}
		v.Array = append(v.Array, val)
	}

	return v, nil
}

func (r *Resp) ReadBulk() (Value, error) {
	v := Value{}
	v.Typ = "Bulk"

	len, _, err := r.ReadInteger()
	if err != nil {
		return v, err
	}

	Bulk := make([]byte, len)
	r.reader.Read(Bulk)
	v.Bulk = string(Bulk)
	r.ReadLine()

	return v, nil
}

func (v Value) Marshal() []byte {

	switch v.Typ {
	case "Array":
		return v.marshalArray()
	case "Bulk":
		return v.marshalBulk()
	case "string":
		return v.marshalString()
	case "null":
		return v.marshalNull()
	case "error":
		return v.marshalError()
	default:
		return []byte{}
	}
}

func (v Value) marshalString() []byte {

	var s []byte
	s = append(s, STRING)
	s = append(s, v.Str...)
	s = append(s, '\r', '\n')

	return s
}

func (v Value) marshalBulk() []byte {

	var s []byte
	s = append(s, BULK)
	s = append(s, strconv.Itoa(len(v.Bulk))...)
	s = append(s, '\r', '\n')
	s = append(s, v.Bulk...)
	s = append(s, '\r', '\n')

	return s
}

func (v Value) marshalArray() []byte {

	len := len(v.Array)
	var s []byte
	s = append(s, ARRAY)
	s = append(s, strconv.Itoa(len)...)
	s = append(s, '\r', '\n')
	for i := 0; i < len; i++ {
		s = append(s, v.Array[i].Marshal()...)
	}

	return s
}

func (v Value) marshalError() []byte {

	var s []byte
	s = append(s, ERROR)
	s = append(s, v.Str...)
	s = append(s, '\r', '\n')

	return s
}

func (v Value) marshalNull() []byte {
	return []byte("$-1\r\n")
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {

	bytes := v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

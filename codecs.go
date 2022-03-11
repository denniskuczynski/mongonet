package mongonet

// Dummy Codec to pass along []byte slice pointers
// Inspired by:
// https://pkg.go.dev/encoding/json#RawMessage
type RawMessageCodec struct{}

// Dereference []byte slice pointer
func (c RawMessageCodec) Marshal(v interface{}) ([]byte, error) {
	rawMessage := v.(*[]byte)
	return *rawMessage, nil
}

// Expect v to be empty []byte slice pointer
func (c RawMessageCodec) Unmarshal(data []byte, v interface{}) error {
	rawMessage := v.(*[]byte)
	*rawMessage = append((*rawMessage)[0:0], data...)
	return nil
}

func (c RawMessageCodec) Name() string {
	return "rawMessageCodec"
}

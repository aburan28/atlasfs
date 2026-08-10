package mds

import (
	"encoding/json"

	"google.golang.org/grpc/encoding"
)

// codecName is the gRPC content-subtype this service speaks. The wire
// encoding is JSON rather than protobuf for a specific, non-architectural
// reason: no protoc is available in the environment this was built in, and
// hand-writing generated protobuf descriptors would be a large amount of
// mechanical code with no bearing on the protocol being demonstrated.
//
// Everything that makes this a real RPC boundary is unaffected by that
// choice — it is real gRPC over real HTTP/2, with real server streaming,
// deadlines, cancellation, and status codes, between processes that share
// no memory. Swapping to protobuf is a codec registration and a set of
// generated structs; it is not a change to the protocol, the service
// shape, or anything below.
const codecName = "json"

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                       { return codecName }

func init() { encoding.RegisterCodec(jsonCodec{}) }

package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"reflect"
	"time"
	"unsafe"

	"github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/address"
	"go.mongodb.org/mongo-driver/mongo/description"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/x/mongo/driver/topology"

	"google.golang.org/grpc"
)

// Exposed internal mongonet functions for example purposes
// send raw bytes to proxy connection
func sendBytes(writer io.Writer, buf []byte) error {
	for {
		written, err := writer.Write(buf)
		if err != nil {
			return fmt.Errorf("error writing to client: %s", err)
		}

		if written == len(buf) {
			return nil
		}

		buf = buf[written:]
	}
}

// https://jira.mongodb.org/browse/GODRIVER-1760 will add the ability to create a topology.Topology from ClientOptions
func extractTopology(mc *mongo.Client) *topology.Topology {
	e := reflect.ValueOf(mc).Elem()
	d := e.FieldByName("deployment")
	if d.IsZero() {
		panic("failed to extract deployment topology")
	}
	d = reflect.NewAt(d.Type(), unsafe.Pointer(d.UnsafeAddr())).Elem() // #nosec G103
	return d.Interface().(*topology.Topology)
}

// Send raw bytes to mongoClient -- modified from original
func RunCommandUsingRawBSON(rawmsg []byte, client *mongo.Client, goctx context.Context) ([]byte, error) {
	topology := extractTopology(client)
	srv, err := topology.SelectServer(goctx, description.ReadPrefSelector(readpref.Primary()))
	if err != nil {
		return nil, err
	}
	conn, err := srv.Connection(goctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := conn.WriteWireMessage(goctx, rawmsg); err != nil {
		return nil, err
	}

	ret, err := conn.ReadWireMessage(goctx, nil)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

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

// mongonet Interceptor factory
// Intercepts initial mongorpc and sends across local gRPC server as middleware
type MyFactory struct {
	conn *grpc.ClientConn
}

func (myf *MyFactory) NewInterceptor(ps *mongonet.ProxySession) (mongonet.ProxyInterceptor, error) {
	return &MyInterceptor{ps, myf.conn}, nil
}

type MyInterceptor struct {
	ps   *mongonet.ProxySession
	conn *grpc.ClientConn
}

func (myi *MyInterceptor) InterceptClientToMongo(m mongonet.Message, previousResult mongonet.SimpleBSON) (
	mongonet.Message,
	mongonet.ResponseInterceptor,
	string,
	address.Address,
	string,
	error,
) {
	in := m.Serialize()
	fmt.Printf("Serialized %v\n", in)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	var out []byte
	err := myi.conn.Invoke(ctx, "/mongorpcToGrpc/Send", &in, &out, grpc.ForceCodec(RawMessageCodec{}))
	if err != nil {
		fmt.Printf("Error %v\n", err)
		return nil, nil, "", "", "", err
	}

	fmt.Printf("Sending to proxy session %v\n", out)
	err = sendBytes(myi.ps.Connection(), out)
	if err != nil {
		fmt.Printf("Error %v\n", err)
		return nil, nil, "", "", "", err
	}

	// already responded, so return nil message
	return nil, nil, "", "", "", nil
}

func (myi *MyInterceptor) Close() {
}
func (myi *MyInterceptor) TrackRequest(mongonet.MessageHeader) {
}
func (myi *MyInterceptor) TrackResponse(mongonet.MessageHeader) {
}
func (myi *MyInterceptor) CheckConnection() error {
	return nil
}
func (myi *MyInterceptor) CheckConnectionInterval() time.Duration {
	return 0
}

func main() {
	// Configuration parsing
	bindHost := flag.String("host", "127.0.0.1", "what to bind to")
	mongoHost := flag.String("mongoHost", "127.0.0.1", "mongo process host")
	bindPort := flag.Int("port", 9999, "what to bind to")
	grpcPort := flag.Int("grpcPort", 50051, "port grpc server is on")
	flag.Parse()

	// Setup MongoClient
	ctx := context.Background()
	opts := options.Client()
	opts.ApplyURI(fmt.Sprintf("mongodb://%s", *mongoHost))
	mongoClient, err := mongo.Connect(ctx, opts)
	if err != nil {
		panic(fmt.Sprint("failed to setup mongoClient: %v", err))
	}
	
	// Set up intermediate gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf("%v:%d", *bindHost, *grpcPort))
	if err != nil {
		panic(fmt.Sprint("failed to listen: %v", err))
	}
	s := grpc.NewServer(grpc.ForceServerCodec(RawMessageCodec{}))
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: "mongorpcToGrpc",
		HandlerType: nil,
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Send",
				Handler: func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
					var in []byte
					if err := dec(&in); err != nil {
						return nil, err
					}
					// interceptor should be nil

					goctx := context.Background()
					fmt.Printf("Sending to mongoClient: %v\n", in)
					out, err := RunCommandUsingRawBSON(in, mongoClient, goctx)
					if err != nil {
						panic(err)
					}
					fmt.Printf("Received from mongoClient: %v\n", out)

					return &out, nil
				},
			},
		},
		Streams: []grpc.StreamDesc{},
	}, nil)

	go func() {
		fmt.Printf("Starting local gRPC server %v\n", lis.Addr())
		if err := s.Serve(lis); err != nil {
			panic(fmt.Sprint("failed to serve: %v", err))
		}
	}()

	// Set up a connection to the server.
	grpcHostPort := fmt.Sprintf("%v:%v", *bindHost, *grpcPort)
	fmt.Printf("Establishing local gRPC connection %v\n", grpcHostPort)
	conn, err := grpc.Dial(grpcHostPort, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// Set up mongonet proxy
	fmt.Printf("Establishing mongonet proxy on %v:%v\n", *bindHost, *bindPort)
	pc := mongonet.NewProxyConfig(*bindHost, *bindPort, "", *bindHost, *grpcPort, "", "", "mongorpc to grpc proxy", false, util.Direct, 5, mongonet.DefaultMaxPoolSize, mongonet.DefaultMaxPoolIdleTimeSec, mongonet.DefaultConnectionPoolHeartbeatIntervalMs)
	pc.InterceptorFactory = &MyFactory{conn}
	pc.MongoSSLSkipVerify = true
	proxy, err := mongonet.NewProxy(pc)
	if err != nil {
		panic(err)
	}

	proxy.InitializeServer()
	if ok, _, _ := proxy.OnSSLConfig(nil); !ok {
		panic("failed to call OnSSLConfig")
	}

	fmt.Println("Running Proxy")
	err = proxy.Run()
	if err != nil {
		panic(err)
	}
}

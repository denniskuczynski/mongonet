package main

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	"github.com/mongodb/mongonet"
	"github.com/mongodb/mongonet/util"

	"github.com/mongodb/slogger/v2/slogger"
	"go.mongodb.org/mongo-driver/mongo/address"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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

// mongonet Interceptor factory
// Intercepts mongorpc and sends to mongonet in gRPC mode
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

	ctx = metadata.AppendToOutgoingContext(ctx, "ServerName", myi.ps.SSLServerName, "RemoteAddr", myi.ps.RemoteAddr().String())

	stream, err := myi.conn.NewStream(ctx, &grpc.StreamDesc{
		StreamName:    "Send",
		ServerStreams: true,
	}, "/mongonet/Send", grpc.ForceCodec(mongonet.RawMessageCodec{}))
	if err != nil {
		fmt.Printf("Could not create stream %v\n", err)
		return nil, nil, "", "", "", err
	}
	if err := stream.SendMsg(&in); err != nil {
		fmt.Printf("Could not send message %v\n", err)
		return nil, nil, "", "", "", err
	}
	if err := stream.CloseSend(); err != nil {
		fmt.Printf("Could not close send %v\n", err)
		return nil, nil, "", "", "", err
	}

	fmt.Printf("Reading responses from gRPC session...\n")
	for {
		var out []byte
		err := stream.RecvMsg(&out)
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("Error receiving message %v\n", err)
			return nil, nil, "", "", "", err
		}
		fmt.Printf("Sending to proxy session %v\n", out)
		err = sendBytes(myi.ps.Connection(), out)
		if err != nil {
			fmt.Printf("Error sending message %v\n", err)
			return nil, nil, "", "", "", err
		}
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

func loadCertificate(certPath string) (*x509.CertPool, error) {
	certPool := x509.NewCertPool()
	certsBytes, err := ioutil.ReadFile(certPath)
	if err != nil {
		return nil, err
	}

	block, blockRemainder := pem.Decode(certsBytes)
	i := 1
	for block != nil {
		if block.Type == "CERTIFICATE" {
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, err
			}
			certPool.AddCert(cert)
		}
		block, blockRemainder = pem.Decode(blockRemainder)
		i++
	}
	return certPool, nil
}

func main() {
	// Configuration parsing
	bindHost := flag.String("host", "127.0.0.1", "what to bind to")
	bindKey := flag.String("key", "", "mongonet TLS certificate file path")
	bindPort := flag.Int("port", 9999, "what to bind to")
	mongoPort := flag.Int("mongoPort", 27017, "what to bind to")
	grpcPort := flag.Int("grpcPort", 9900, "port grpc server is on")
	flag.Parse()

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
	pc := mongonet.NewProxyConfig(false, *bindHost, *bindPort, "", *bindHost, *mongoPort, "", "", "mongorpc to grpc proxy", false, util.Direct, 5, mongonet.DefaultMaxPoolSize, mongonet.DefaultMaxPoolIdleTimeSec, mongonet.DefaultConnectionPoolHeartbeatIntervalMs)
	pc.InterceptorFactory = &MyFactory{conn}
	pc.LogLevel = slogger.DEBUG
	pc.MongoSSLSkipVerify = true
	if *bindKey != "" {
		fmt.Printf("Using TLS key %v\n", *bindKey)
		pc.UseSSL = true
		pc.SSLKeys = []mongonet.SSLPair{
			{*bindKey, *bindKey, "fallback"},
		}
	}

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

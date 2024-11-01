package server

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	api "github.com/alphaleph/yojimbo/api/v1"
)

type CommitLog interface {
	Append(*api.Record) (uint64, error)
	Read(uint64) (*api.Record, error)
}

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type subjectContextKey struct{}

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

const (
	wildcard      = "*"
	produceAction = "produce"
	consumeAction = "consume"
)

var _ api.LogServer = (*grpcServer)(nil)

func NewGRPCServer(c *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(grpc_auth.StreamServerInterceptor(authenticate)),
		),
		grpc.UnaryInterceptor(
			grpc_middleware.ChainUnaryServer(grpc_auth.UnaryServerInterceptor(authenticate)),
		),
	)
	gsrv := grpc.NewServer(opts...)
	srv, err := newgrpcServer(c)
	if err != nil {
		return nil, err
	}
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}

type grpcServer struct {
	api.UnimplementedLogServer
	*Config
}

func newgrpcServer(c *Config) (*grpcServer, error) {
	srv := &grpcServer{
		Config: c,
	}
	return srv, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := s.Authorizer.Authorize(ctx.Value(subjectContextKey{}).(string), wildcard, consumeAction); err != nil {
		return nil, err
	}

	rec, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: rec}, nil
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			case api.ErrOffsetOutOfRange:
				continue
			default:
				return err
			}

			if err = stream.Send(res); err != nil {
				return err
			}
			req.Offset++
		}
	}
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if err := s.Authorizer.Authorize(ctx.Value(subjectContextKey{}).(string), wildcard, produceAction); err != nil {
		return nil, err
	}

	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil, err
		}
		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return nil, err
		}
		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext()
	if !ok {
		return ctx, status.New(codes.Unknown, "Peer info not found").Err()
	}
	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}
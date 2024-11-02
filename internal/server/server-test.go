package server

import (
	"context"
	"flag"
	"net"
	"os"
	"testing"
	"time"

	"github.com/alphaleph.yojimbo/internal/config"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"

	api "github.com/alphaleph/yojimbo/api/v1"
	auth "github.com/alphaleph/yojimbo/internal/auth"
	"github.com/alphaleph/yojimbo/internal/config"
	"github.com/alphaleph/yojimbo/internal/log"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		guestClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from log": testProduceConsume,
		"produce/consume stream":                testProduceConsumeStream,
		"consume exceeding log boundary fails":  testConsumePastBoundary,
		"unauthorized fails":                    testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, guestClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, guestClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (api.LogClient, api.LogClient, *Config, func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	generateClient := func(crtPath, keyPath string) (*grpc.ClientConn, api.LogClient, []grpc.DialOption) {
		clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		require.NoError(t, err)
		clientCreds := credentials.NewTLS(clientTLSConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}
		cc, err := grpc.NewClient(l.Addr().String(), opts...)
		require.NoError(t, err)
		client := api.NewLogClient(cc)
		return cc, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ := generateClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)
	var guestConn *grpc.ClientConn
	guestConn, guestClient, _ := generateClient(
		config.GuestClientCertFile,
		config.GuestClientKeyFile,
	)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	cfg := &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}

	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := os.CreateTemp("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("Metrics Log File: %s", metricsLogFile.Name())
		tracesLogFile, err := os.CreateTemp("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("Traces Log File: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: time.Second,
		})
		require.NoError(t, err)
		err = telemetryExporter.Start()
		require.NoError(t, err)
	}

	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return rootClient, guestClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		guestConn.Close()
		l.Close()
		clog.Remove()
		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond) // Some time to flush data to disk
			telemetryExporter.Stop()
			telemetryExporter.Close()
		}
	}
}

func testProduceConsume(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()
	expected := &api.Record{
		Value: []byte("hello world"),
	}
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: expected,
		},
	)
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset,
	})
	require.NoError(t, err)
	require.Equal(t, expected.Value, consume.Record.Value)
	require.Equal(t, expected.Offset, consume.Record.Offset)
}

func testProduceConsumeStream(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()
	recs := []*api.Record{
		{
			Value:  []byte("hello world 0"),
			Offset: 0,
		},
		{
			Value:  []byte("hello world 1"),
			Offset: 1,
		},
	}
	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for {
			for offset, rec := range recs {
				err = stream.Send(&api.ProduceRequest{
					Record: rec,
				})
				require.NoError(t, err)
				res, err := stream.Recv()
				require.NoError(t, err)
				if res.Offset != uint64(offset) {
					t.Fatalf("Got offset: %d, Expected: %d", res.Offset, offset)
				}
			}
		}
	}
	{
		stream, err = client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Offset: 0},
		)
		require.NoError(t, err)
		for i, rec := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  rec.Value,
				Offset: uint64(i),
			})
		}
	}
}

func testConsumePastBoundary(t *testing.T, client, _ api.LogClient, config *Config) {
	ctx := context.Background()
	rec := &api.Record{
		Value: []byte("hello world"),
	}
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: rec,
		},
	)
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Offset: produce.Offset + 1,
	})
	if consume != nil {
		t.Fatal("Consume is not nil")
	}
	got := status.Code(err)
	expected := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != expected {
		t.Fatalf("got err: %v, expected: %v", got, expected)
	}
}

func testUnauthorized(t *testing.T, _, client api.LogClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(
		ctx,
		&api.ProduceRequest{
			Record: &api.Record{
				Value: []byte("hello world"),
			},
		},
	)
	if produce != nil {
		t.Fatalf("Produce response should be nil")
	}
	gotCode, expectedCode := status.Code(err), codes.PermissionDenied
	if gotCode != expectedCode {
		t.Fatalf("got code: %d, expected: %d", gotCode, expectedCode)
	}
	consume, err := client.Consume(
		ctx,
		&api.ConsumeRequest{Offset: 0},
	)
	if consume != nil {
		t.Fatalf("Consume response should be nil")
	}
	gotCode, expectedCode = status.Code(err), codes.PermissionDenied
	if gotCode != expectedCode {
		t.Fatalf("got code: %d, expected: %d", gotCode, expectedCode)
	}
}

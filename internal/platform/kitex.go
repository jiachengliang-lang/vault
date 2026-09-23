package platform

import (
	"net"
	"time"

	"github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/rpcinfo"
	"github.com/cloudwego/kitex/server"
	"github.com/kitex-contrib/obs-opentelemetry/tracing"
)

// ServerOptions is the standard setup for a Kitex server: its name, address,
// trace propagation (reads the caller's trace context from TTHeader) and RED metrics.
func ServerOptions(service string, addr *net.TCPAddr) []server.Option {
	return []server.Option{
		server.WithServiceAddr(addr),
		server.WithServerBasicInfo(&rpcinfo.EndpointBasicInfo{ServiceName: service}),
		server.WithSuite(tracing.NewServerSuite()),
		RPCMetrics(service),
	}
}

// ClientOptions is the standard setup for calling another service. Every call gets a deadline:
// without one, a hung downstream ties up the caller's goroutines until it falls over too.
func ClientOptions(caller, hostPort string) []client.Option {
	return []client.Option{
		client.WithHostPorts(hostPort),
		client.WithClientBasicInfo(&rpcinfo.EndpointBasicInfo{ServiceName: caller}),
		client.WithSuite(tracing.NewClientSuite()),
		client.WithRPCTimeout(2 * time.Second),
		client.WithConnectTimeout(500 * time.Millisecond),
	}
}

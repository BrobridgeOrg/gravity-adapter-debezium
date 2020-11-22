package app

import (
	grpc_connection_pool "github.com/cfsghost/grpc-connection-pool"
)

type App interface {
	GetGRPCPool() *grpc_connection_pool.GRPCPool
}

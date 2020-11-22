package instance

import (
	"time"

	grpc_connection_pool "github.com/cfsghost/grpc-connection-pool"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
)

func (a *AppInstance) initGRPCPool() error {

	host := viper.GetString("dsa.host")

	log.WithFields(log.Fields{
		"host": host,
	}).Info("Initializing gRPC pool")

	options := &grpc_connection_pool.Options{
		InitCap:     1,
		MaxCap:      1,
		DialTimeout: time.Second * 20,
	}

	// Initialize connection pool
	p, err := grpc_connection_pool.NewGRPCPool(host, options, grpc.WithInsecure())
	if err != nil {
		return err
	}

	if p == nil {
		return err
	}

	a.grpcPool = p
	return nil
}

func (a *AppInstance) GetGRPCPool() *grpc_connection_pool.GRPCPool {
	return a.grpcPool
}

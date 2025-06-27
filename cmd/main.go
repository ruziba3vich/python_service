package main

import (
	"context"
	"log"
	"net"

	"github.com/ruziba3vich/python_service/genprotos/genprotos/compiler_service"
	"github.com/ruziba3vich/python_service/internal/service"
	"github.com/ruziba3vich/python_service/pkg/lgg"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

func NewNetListener(lc fx.Lifecycle) (net.Listener, error) {
	lis, err := net.Listen("tcp", ":702")
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return lis.Close()
		},
	})

	return lis, nil
}

func main() {
	fx.New(
		fx.Provide(
			NewNetListener,
			lgg.NewLogger,
			service.NewPythonExecutorServer,
		),
		fx.Invoke(func(lc fx.Lifecycle, lis net.Listener, server *service.PythonExecutorServer) {
			s := grpc.NewServer()
			compiler_service.RegisterCodeExecutorServer(s, server)

			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					log.Println("gRPC server starting on :7771...")
					go func() {
						if err := s.Serve(lis); err != nil {
							log.Printf("gRPC server failed: %v", err)
						}
					}()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					log.Println("Stopping gRPC server...")
					s.GracefulStop()
					return nil
				},
			})
		}),
	).Run()
}

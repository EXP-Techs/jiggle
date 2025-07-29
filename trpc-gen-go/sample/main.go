package main

import (
	"context"
	"fmt"
	"log"
	"time"

	// These would be your actual import paths
	"github.com/exp-techs/jiggle/trpc-gen-go/driver"
	user_v1 "github.com/exp-techs/jiggle/trpc-gen-go/sample/gen"
)

// --- Server Implementation ---
// This struct implements the generated user_v1.UserServiceServer interface.
type server struct {
	instanceID string
}

func (s *server) GetUser(ctx context.Context, req *user_v1.GetUserRequest) (*user_v1.User, error) {
	log.Printf("[%s] Received GetUser RPC: %+v", s.instanceID, req)
	return &user_v1.User{UserId: req.UserId, Name: "Alice", Email: "alice@example.com"}, nil
}

func (s *server) CreateUser(ctx context.Context, req *user_v1.CreateUserRequest) (*user_v1.User, error) {
	log.Printf("[%s] Received CreateUser RPC: %+v", s.instanceID, req)
	// In a real app, you would create a user in the database and return the full user object.
	return &user_v1.User{UserId: "u-new-123", Name: req.Name, Email: req.Email}, nil
}

func (s *server) UpdateUser(ctx context.Context, req *user_v1.UpdateUserRequest) (*user_v1.User, error) {
	log.Printf("[%s] Received UpdateUser RPC: %+v", s.instanceID, req)
	return &user_v1.User{UserId: req.UserId, Name: req.Name, Email: req.Email}, nil
}

func (s *server) HandleUserCreated(ctx context.Context, event *user_v1.User) error {
	log.Printf("[%s] Received event: UserCreated %+v", s.instanceID, event)
	return nil
}

func (s *server) HandleUserUpdated(ctx context.Context, event *user_v1.User) error {
	log.Printf("[%s] Received event: UserUpdated %+v", s.instanceID, event)
	return nil
}

func (s *server) HandleUserDeleted(ctx context.Context, event *user_v1.GetUserRequest) error {
	log.Printf("[%s] Received event: UserDeleted %+v", s.instanceID, event)
	return nil
}

// --- Main Application ---
func main() {
	brokerURI := "trpc://admin:admin1234@localhost:8772"

	// --- Start two instances of the UserService ---
	// They will both handle RPCs (load balanced) and Events (competing consumers).
	for i := 1; i <= 2; i++ {
		instanceID := fmt.Sprintf("UserService-%d", i)
		go func() {
			drv, err := driver.Connect(brokerURI)
			if err != nil {
				log.Fatalf("[%s] failed to connect: %v", instanceID, err)
			}
			defer drv.Close()

			// Register the server implementation using the generated function.
			// The subscriber ID ensures both instances are part of the same consumer group for events.
			user_v1.RegisterUserServiceServer(drv, &server{instanceID: instanceID})
			log.Printf("[%s] Successfully registered handlers.", instanceID)
			select {}
		}()
	}

	time.Sleep(2 * time.Second)

	// --- Start the Requester (Client) ---
	go func() {
		drv, err := driver.Connect(brokerURI)
		if err != nil {
			log.Fatalf("[Client] failed to connect: %v", err)
		}
		defer drv.Close()

		// Create a type-safe client using the generated constructor.
		client := user_v1.NewUserServiceClient(drv)

		log.Println("[Client] --- Calling CreateUser RPC ---")
		newUser, err := client.CreateUser(context.Background(), &user_v1.CreateUserRequest{Name: "Bob", Email: "bob@example.com"})
		if err != nil {
			log.Fatalf("[Client] CreateUser failed: %v", err)
		}
		log.Printf("[Client] Received CreateUser response: %+v", newUser)

		log.Println("[Client] --- Publishing UserCreated Event ---")
		err = client.PublishUserCreated(context.Background(), newUser)
		if err != nil {
			log.Fatalf("[Client] Failed to publish UserCreated event: %v", err)
		}
		log.Println("[Client] Event published successfully.")
		select {}
	}()

	log.Println("Example services are running. Press Ctrl+C to exit.")
	select {}
}

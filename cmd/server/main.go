package main

// cmd/server/main.go
import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	fmt.Println("Connection successful")
	gamelogic.PrintServerHelp()
	
	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}
	defer ch.Close()

	// Declare and bind queue for game logs
	_, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		routing.GameLogSlug+".*",
		pubsub.SimpleQueueDurable,
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gamelogic.PrintServerHelp()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGTERM)
	
	for {
		select {
		case <-done:
			fmt.Println("Shutting down...") 
			return 
		default:
			words := gamelogic.GetInput()
			if len(words) == 0 {
				continue 
			}

			switch words[0] {
			case "pause":
				fmt.Println("Publishing paused game state")
				err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
				if err != nil {
					log.Printf("cound not publish time: %v", err)
				}
			case "resume":
				fmt.Println("Publishing resusmes game state")
				err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
				if err != nil {
					log.Printf("could not publish time: %v", err)
				}
			case "quit":
				fmt.Println("Exiting...")
				return 
			default:
				fmt.Println("Unknown command")
			}
		}
	}
}

package main
// cmd/client/main.go
import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connStr := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connStr)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game client connect to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("could not get username: %v", err)
	}

	_, queue, err := pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+username,
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", queue.Name)

	gs := gamelogic.NewGameState(username)

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
			case "spawn":
				err := gs.CommandSpawn(words)
				if err != nil {
					fmt.Println(err)
					continue 
				}
			case "move":
				_, err := gs.CommandMove(words) 
				if err != nil {
					fmt.Println("Move failed")
				} else {
					fmt.Println("Move successful")
				}
			case "status":
				gs.CommandStatus()
			case "help":
				gamelogic.PrintClientHelp()
			case "spam":
				fmt.Println("Spamming not allowed yet!")
			case "quit":
				gamelogic.PrintQuit()
				return 
			default:
				fmt.Println("Unknown command")
			}
		}
	}
}

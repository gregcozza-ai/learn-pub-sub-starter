package main

// cmd/client/main.go
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
	
	gs := gamelogic.NewGameState(username)
	
	// Create channel for publishing moves and war messages 
	pubCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel for publishing: %v", err)
	}
	defer pubCh.Close()
	
	// Subscribe to pause messages
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+gs.GetUsername(),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gs),
	)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	// Subscribe to moves from other players
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+gs.GetUsername(),
		routing.ArmyMovesPrefix+".*",
		pubsub.SimpleQueueTransient,
		handlerMove(gs, pubCh),
	)
	if err != nil {
		log.Fatalf("could not subscribe to army moves: %v", err)
	}

	// Subscribe to war messages (durable queue)
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix, // Durable queue name
		routing.WarRecognitionsPrefix+".*",
		pubsub.SimpleQueueDurable, // Durable queue
		handlerWar(gs),
	)
	if err != nil {
		log.Fatalf("could not subscribe to war messages: %v", err)
	}

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
				mv, err := gs.CommandMove(words)
				if err != nil {
					fmt.Println("Move failed")
				} else {
					fmt.Println("Move successful")
					// Publish move to army_moves.<username>
					err = pubsub.PublishJSON(
						pubCh,
						routing.ExchangePerilTopic,
						routing.ArmyMovesPrefix+"."+mv.Player.Username,
						mv,
					)
					if err != nil {
						fmt.Println("Failed to publish move:", err)
						continue
					} else {
						fmt.Println("Move published successfully")
						fmt.Printf("Moved %v units to %s\n", len(mv.Units), mv.ToLocation)
					}
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


package main

// cmd/client/main.go
import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

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
		handlerWar(gs, pubCh),
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
				if len(words) < 2 {
					fmt.Println("Usage: spam <count>")
					continue 
				}
				count, err := strconv.Atoi(words[1])
				if err != nil {
					fmt.Println("Invalid count:", err)
					continue 
				}
				for i := 0; i < count; i++ {
					msg := gamelogic.GetMaliciousLog()
					err := publishGameLog(pubCh, username, msg)
					if err != nil {
						fmt.Println("Failed to publish log:", err)
					}
				}
				fmt.Printf("Spammed %d logs\n", count)
			case "quit":
				gamelogic.PrintQuit()
				return
			default:
				fmt.Println("Unknown command")
			}
		}
	}
}

func publishGameLog(publishCh *amqp.Channel, username, msg string) error {
	return pubsub.PublishGob(
		publishCh,
		routing.ExchangePerilTopic,
		routing.GameLogSlug+"."+username,
		routing.GameLog{
			Username:		username,
			CurrentTime:	time.Now(),
			Message:		msg,
		},
	)
}
package main

import (
	"fmt"
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
				fmt.Println("Sending pause message...")
				err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
				if err != nil {
					panic(err)
				}
			case "resume":
				fmt.Println("Sending resume message...")
				err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
				if err != nil {
					panic(err)
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

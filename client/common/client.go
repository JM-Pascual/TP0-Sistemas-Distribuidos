package common

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return err
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {

	// Creo el canal mediante el cual voy a comunicar las señales de interrupción
	signalsChannel := make(chan os.Signal, 1)

	// Creo el canal mediante el cual anuncio el fin del client loop
	finishChannel := make(chan bool, 1)

	// Utilizo el canal como medio de comunicación para finalizar el client loop
	signal.Notify(signalsChannel, syscall.SIGINT, syscall.SIGTERM)

	// Creo un goroutine que se encargue de recibir las señales de interrupción
	go func() {
		signal := <-signalsChannel
		if c.conn != nil {
			c.conn.Close()
		}
		log.Infof("action: signal_handler | result: success | signal: %v | client_id: %v", signal, c.config.ID)
		finishChannel <- true
	}()

	// Defino la variable que va a contener el número de mensaje enviado fuera del loop
	msgID := 1

	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed
	for {
		select {
		case <-finishChannel:
			log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
			return
		default:
			if msgID > c.config.LoopAmount {
				finishChannel <- true
				break
			}
			msgID = msgID + 1

			// Create the connection the server in every loop iteration. Send an
			err := c.createClientSocket()

			if err != nil {
				log.Errorf("action: createClientSocket | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				finishChannel <- true
				break
			}

			// TODO: Modify the send to avoid short-write
			fmt.Fprintf(
				c.conn,
				"[CLIENT %v] Message N°%v\n",
				c.config.ID,
				msgID,
			)
			msg, err := bufio.NewReader(c.conn).ReadString('\n')
			c.conn.Close()

			if err != nil {
				log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				return
			}

			log.Infof("action: receive_message | result: success | client_id: %v | msg: %v",
				c.config.ID,
				msg,
			)

			// Wait a time between sending one message and the next one
			time.Sleep(c.config.LoopPeriod)
		}
	}
}

package common

import (
	"bufio"
	"fmt"
	"net"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const (
	// Constants used to define the fields of the bet
	BET_USER_NAME     = "NOMBRE"
	BET_USER_LASTNAME = "APELLIDO"
	BET_USER_DOCUMENT = "DOCUMENTO"
	BET_USER_BIRTH    = "NACIMIENTO"
	BET_NUMBER        = "NUMERO"
)

const (
	END_OF_MESSAGE_DELIMITER = '\n'
)

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
}

// Client Entity that encapsulates how
type Client struct {
	config  ClientConfig
	conn    net.Conn
	BetInfo map[string]string
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig, betInfo map[string]string) *Client {
	client := &Client{
		config:  config,
		BetInfo: betInfo,
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

// Function to free resources used by the client
// Exported to be used by the main function
func (c *Client) FreeResources() {
	if c.conn != nil {
		c.conn.Close()
	}
}

// The format for serialized bet info is:
// field1#field2#...#field-n@
// Meaning that the data separator is a hash (#) and the end of the line is a new line character (\n)

func (c *Client) getSerializedBetInfo() string {
	return fmt.Sprintf(
		"%s#%s#%s#%s#%s#%s\n",
		c.config.ID,
		c.BetInfo[BET_USER_NAME],
		c.BetInfo[BET_USER_LASTNAME],
		c.BetInfo[BET_USER_DOCUMENT],
		c.BetInfo[BET_USER_BIRTH],
		c.BetInfo[BET_NUMBER],
	)
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop(finishChannel chan bool) {
	// The message ID is defined outside the loop, and incremented inside of it while there are iterations remaining
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

			// Create the connection the server in every loop iteration.
			// Pickup errors if any
			err := c.createClientSocket()

			if err != nil {
				log.Errorf("action: createClientSocket | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				finishChannel <- true
				break
			}

			// Send the serialized bet info to the server
			// First get the byte len of the serialized bet info
			serializedMessage := c.getSerializedBetInfo()
			totalBytesLen := len(serializedMessage)
			bytesSent := 0

			bytesSent, err = fmt.Fprintf(c.conn, "%s", serializedMessage)

			for bytesSent < totalBytesLen {
				bytesSent, err = fmt.Fprintf(c.conn, "%s", serializedMessage[bytesSent:])
				if err != nil {
					log.Errorf("action: send_serialized_message | result: fail | client_id: %v | error: %v",
						c.config.ID,
						err,
					)
					return
				}
			}

			_, err = bufio.NewReader(c.conn).ReadString(END_OF_MESSAGE_DELIMITER)
			c.conn.Close()

			if err != nil {
				log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				return
			}

			log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v",
				c.BetInfo[BET_USER_DOCUMENT],
				c.BetInfo[BET_NUMBER],
			)

			// Message ID incremented after successful loop iteration
			msgID = msgID + 1

			// Wait a time between sending one message and the next one
			time.Sleep(c.config.LoopPeriod)
		}
	}
}

package common

import (
	"bufio"
	"fmt"
	"github.com/op/go-logging"
	"io"
	"net"
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

const END_OF_BATCH_DELIMITER = '@'
const END_OF_BATCH_BET_DELIMITER = '\n'
const BET_FIELD_DELIMITER = '#'
const END_OF_BETS_UPLOAD_MESSAGE = "AWAITING_RESULTS"
const EOF_MESSAGE = "EOF"
const MAX_MESSAGE_BYTE_SIZE = (8192 - 8) // 8KB - 8 bytes for the end of batch delimiter

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	BatchSize     int
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

func (c *Client) getSerializedBetInfo(betInfo map[string]string) string {
	return fmt.Sprintf(
		"%s#%s#%s#%s#%s#%s\n",
		c.config.ID,
		betInfo[BET_USER_NAME],
		betInfo[BET_USER_LASTNAME],
		betInfo[BET_USER_DOCUMENT],
		betInfo[BET_USER_BIRTH],
		betInfo[BET_NUMBER],
	)
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop(finishChannel chan bool, betsInfo chan map[string]string) {
	// Create an aux variable to store the messages that were not sent due to the max size being reached
	// If present, the message will be sent in the next iteration as the head of the line message
	unsentMessageDueToMaxSize := ""
	// Variable used for login the number of bets sent to the server via this agency
	sentBets := 0
	for {
		select {
		case <-finishChannel:
			log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
			return
		default:
			// The initial value of the message is the unsent message from a previous iteration
			messageToSend := unsentMessageDueToMaxSize
			messagesAddedToBatch := 0

			if unsentMessageDueToMaxSize != "" {
				messagesAddedToBatch = 1
				unsentMessageDueToMaxSize = ""
			}

			// Get bets until either:
			// - The max batch size is reached
			// - The max message size is reached
			// - The EOF message is received
			shouldSkipIteration := false
			for {
				betInfo := <-betsInfo

				if betInfo[EOF_MESSAGE] == EOF_MESSAGE {
					finishChannel <- true
					shouldSkipIteration = true
					break
				}

				serializedMessage := c.getSerializedBetInfo(betInfo)

				// If the byte size of the message to send is greater than the max size, store the last message and end the loop
				if len(messageToSend)+len(serializedMessage) > MAX_MESSAGE_BYTE_SIZE {
					unsentMessageDueToMaxSize = serializedMessage
					break
				}

				messageToSend += serializedMessage
				messagesAddedToBatch++
				if messagesAddedToBatch >= c.config.BatchSize {
					break
				}
			}

			if shouldSkipIteration {
				break
			}

			// Add the end of batch delimiter to the message
			messageToSend += string(END_OF_BATCH_DELIMITER)

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
			totalBytesLen := len(messageToSend)
			bytesSent := 0

			bytesSent, err = fmt.Fprintf(c.conn, "%s", messageToSend)

			for bytesSent < totalBytesLen {
				bytesSent, err = fmt.Fprintf(c.conn, "%s", messageToSend[bytesSent:])
				if err != nil {
					log.Errorf("action: send_serialized_message | result: fail | client_id: %v | error: %v",
						c.config.ID,
						err,
					)
					return
				}
			}

			_, err = bufio.NewReader(c.conn).ReadString(END_OF_BATCH_DELIMITER)
			c.conn.Close()

			if err != nil && err != io.EOF {
				log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				return
			}

			sentBets += messagesAddedToBatch
		}

		log.Infof("action: apuesta_validada | result: success | cantidad: %v",
			sentBets,
		)
	}
}

func (c *Client) AwaitForLotteryResults() {
	err := c.createClientSocket()

	if err != nil {
		log.Errorf("action: createClientSocket | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return
	}

	awaitingResultsMessage := fmt.Sprintf("%s#%s#\n@", c.config.ID, END_OF_BETS_UPLOAD_MESSAGE)

	totalBytesLen := len(awaitingResultsMessage)
	bytesSent := 0

	bytesSent, err = fmt.Fprintf(c.conn, "%s", awaitingResultsMessage)

	for bytesSent < totalBytesLen {
		bytesSent, err = fmt.Fprintf(c.conn, "%s", awaitingResultsMessage[bytesSent:])
		if err != nil {
			log.Errorf("action: send_awaiting_message | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
			return
		}
	}

	// The client will wait for the lottery results to be sent by the server
	lotteryResults, err := bufio.NewReader(c.conn).ReadString(END_OF_BATCH_DELIMITER)

	if err != nil && err != io.EOF {
		log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return
	}

	// Parses the received message knowing the format is:
	// "amount_of_winners#\n"
	amountOfWinners := lotteryResults[:len(lotteryResults)-2]

	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %v",
		amountOfWinners,
	)
}

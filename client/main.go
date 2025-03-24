package main

import (
	"bufio"
	"fmt"
	"github.com/op/go-logging"
	"github.com/spf13/viper"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/common"
)

const (
	// Constants used to define the fields of the bet
	AGENCY_NUMBER     = "CLI_ID"
	BET_USER_NAME     = "NOMBRE"
	BET_USER_LASTNAME = "APELLIDO"
	BET_USER_DOCUMENT = "DOCUMENTO"
	BET_USER_BIRTH    = "NACIMIENTO"
	BET_NUMBER        = "NUMERO"
)

const BETS_FILE_PATH = "agency-data.csv"
const FILE_FIELDS_SEPARATOR = ","
const BETS_CHANNEL_BUFFER_SIZE = 20
const EOF_MESSAGE = "EOF"

var log = logging.MustGetLogger("log")

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("server", "address")
	v.BindEnv("log", "level")
	v.BindEnv("batch", "maxAmount")

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}

// PrintConfig Print all the configuration parameters of the program.
// For debugging purposes only
func PrintConfig(v *viper.Viper) {
	log.Infof("action: config | result: success | client_id: %s | server_address: %s | log_level: %s | batch_maxAmount: %d",
		v.GetString("id"),
		v.GetString("server.address"),
		v.GetString("log.level"),
		v.GetInt("batch.maxAmount"),
	)
}

func signalHandler(client *common.Client, signalsChannel chan os.Signal, finishChannel chan bool) {
	signal := <-signalsChannel
	client.FreeResources()
	log.Infof("action: signal_handler | result: success | signal: %v ", signal)
	finishChannel <- true
}

func parseBet(betRawData []string) map[string]string {
	// Parses the bet info from the raw data read from the file and returns a map
	betInfo := make(map[string]string)

	betInfo[AGENCY_NUMBER] = os.Getenv(AGENCY_NUMBER)
	betInfo[BET_USER_NAME] = betRawData[0]
	betInfo[BET_USER_LASTNAME] = betRawData[1]
	betInfo[BET_USER_DOCUMENT] = betRawData[2]
	betInfo[BET_USER_BIRTH] = betRawData[3]
	betInfo[BET_NUMBER] = betRawData[4]
	betInfo[EOF_MESSAGE] = ""

	// If any of the required fields is not complete, return an error
	for key, value := range betInfo {
		if value == "" && key != EOF_MESSAGE {
			log.Criticalf("action: get_bet_env_variables | result: fail | error: %s not set", key)
			os.Exit(1)
		}
	}

	return betInfo
}

func parseBetFile(filePath string, betsChannel chan map[string]string) {
	// Reads the file and pushes the bet info to the betsChannel following a Producer / Consumer pattern
	// The betsChannel is buffered to avoid overflowing the memory in cases where the consumer is slower than the producer or the file is very big

	file, err := os.Open(filePath)
	if err != nil {
		log.Criticalf("action: open_bet_file | result: fail | error: %v", err)
		os.Exit(1)
	}
	// Closes the file when the function ends or exits
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		betRawData := strings.Split(scanner.Text(), FILE_FIELDS_SEPARATOR)
		betInfo := parseBet(betRawData)
		betsChannel <- betInfo
	}

	// Send EOF message to the betsChannel to notify the client loop that there are no more bets to process
	betsChannel <- map[string]string{EOF_MESSAGE: EOF_MESSAGE}

	if err := scanner.Err(); err != nil {
		log.Criticalf("action: read_bets_file | result: fail | error: %v", err)
		os.Exit(1)
	}
}

func main() {
	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
	}

	// Print program config with debugging purposes
	PrintConfig(v)

	// Channel used to receive signals
	signalsChannel := make(chan os.Signal, 1)

	// Channel used to announce the end of the client loop
	finishChannel := make(chan bool, 1)

	// Assign the signalsChannel to receive SIGINT and SIGTERM signals
	signal.Notify(signalsChannel, syscall.SIGINT, syscall.SIGTERM)

	// Channel used to send the parsed bets to the client loop
	betsChannel := make(chan map[string]string, BETS_CHANNEL_BUFFER_SIZE)

	clientConfig := common.ClientConfig{
		ServerAddress: v.GetString("server.address"),
		ID:            v.GetString("id"),
		BatchSize:     v.GetInt("batch.maxAmount"),
	}

	client := common.NewClient(clientConfig)

	// Create a goroutine to handle signals and notify the client loop to finish
	go signalHandler(client, signalsChannel, finishChannel)

	// Go routine in charge of parsing the bets file and sending the parsed bets to the client loop
	go parseBetFile(BETS_FILE_PATH, betsChannel)

	client.StartClientLoop(finishChannel, betsChannel)
}

import signal
import socket
import logging

from .utils import Bet, store_bets

MAX_RECV_BUFFER_SIZE = 1024
FIELD_DELIMITER = "#"
END_OF_BET_DELIMITER = "\n"
END_OF_BATCH_DELIMITER = "@"

CLIENT_ID_INDEX = 0
BET_USER_NAME_INDEX = 1
BET_USER_LASTNAME_INDEX = 2
BET_USER_DOCUMENT_INDEX = 3
BET_USER_BIRTH_INDEX = 4
BET_NUMBER_INDEX = 5


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._server_working = False
        self._total_bets_registered = 0

        # Declaration of the SIGTERM handler
        signal.signal(signal.SIGTERM, self.graceful_shutdown)

        # Declaration of the SIGINT handler
        signal.signal(signal.SIGINT, self.graceful_shutdown)

    def graceful_shutdown(self, signum, frame):
        # Closure of the server socket
        self._server_socket.close()
        # Log the shutdown action
        logging.info('action: graceful_shutdown | result: success | signal number: {}'.format(signum))

    def run(self):
        """
        Agency Server loop

        The server will listen and accept new connections from clients
        Then the bets reported will be stored
        """

        self._server_working = True

        try:
            while self._server_working:
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)
        except OSError as e:
            logging.error(f"action: accepting new connections | result: fail | error: {e}")

    def _recv_all_bet_data(self, client_sock):
        """
        Receives all the data from the client, handling short-reads
        """
        raw_bet_data = bytes(client_sock.recv(MAX_RECV_BUFFER_SIZE))

        while raw_bet_data[-1] != ord(END_OF_BATCH_DELIMITER):
            raw_bet_data += bytes(client_sock.recv(MAX_RECV_BUFFER_SIZE))

        return raw_bet_data

    def _parse_submitted_bets(self, raw_bets):
        """
        Decodes the message received from the client

        The message received from the client is a string with the following format:
        "first_name#last_name#document#birthdate#bet_number\n"

        This function receives the message and returns a dictionary with the following keys:
        - first_name: str
        - last_name: str
        - document: str
        - birthdate: str
        - number: str
        """

        return [bet.split(FIELD_DELIMITER) for bet in raw_bets[:-1].split(END_OF_BET_DELIMITER)]

    def _build_bet_object(self, decoded_message_array):
        """
        Builds a Bet object from the decoded message array

        The message array is a list with the following format:
        [agency_number, first_name, last_name, document, birthdate, number]

        This function receives the message array and returns a Bet object
        """

        return Bet(
            decoded_message_array[CLIENT_ID_INDEX],
            decoded_message_array[BET_USER_NAME_INDEX],
            decoded_message_array[BET_USER_LASTNAME_INDEX],
            decoded_message_array[BET_USER_DOCUMENT_INDEX],
            decoded_message_array[BET_USER_BIRTH_INDEX],
            decoded_message_array[BET_NUMBER_INDEX]
        )

    def _recv_all_bets(self, client_sock):
        """
        Receives all the data from the client, and parses it into single bets
        """
        raw_batch_data = self._recv_all_bet_data(client_sock)

        raw_bets_data = raw_batch_data.decode('utf-8')[:-1]

        bets_data = self._parse_submitted_bets(raw_bets_data)

        return bets_data



    def _send_all_batch_confirmation_data(self, client_sock, bets_received):
        """
        Sends the confirmation message to the client to let them know the batch was received
        """

        encoded_message = bytes(f"{FIELD_DELIMITER.join([f'Success on receiving {bets_received} bets'])}{END_OF_BET_DELIMITER}{END_OF_BATCH_DELIMITER}".encode('utf-8'))
        message_byte_len = len(encoded_message)

        total_bytes_sent = client_sock.send(encoded_message)

        while total_bytes_sent < message_byte_len:
            total_bytes_sent += client_sock.send(encoded_message[total_bytes_sent:])

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            bets_received = self._recv_all_bets(client_sock)

            store_bets([self._build_bet_object(bet) for bet in bets_received])

            bets_stored = len(bets_received)
            self._total_bets_registered += bets_stored

            logging.info(f'action: apuesta_recibida | result: success | cantidad: {bets_stored}.')

            self._send_all_batch_confirmation_data(client_sock, bets_stored)

        except OSError as e:
            logging.error(f"action: apuesta_recibida | result: fail | cantidad: {self._total_bets_registered} | error: {e}")
        finally:
            logging.info(f'action: apuestas_recibidas | result: success | cantidad: {self._total_bets_registered}.')
            client_sock.close()

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()
        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c

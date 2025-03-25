import os
import signal
import socket
import logging

from .utils import Bet, store_bets, load_bets, has_won

MAX_RECV_BUFFER_SIZE = 1024
FIELD_DELIMITER = "#"
END_OF_BET_DELIMITER = "\n"
END_OF_BATCH_DELIMITER = "@"
AWAITING_RESULTS_MESSAGE = "AWAITING_RESULTS"

CLIENT_ID_INDEX = 0
BET_USER_NAME_INDEX = 1
BET_USER_LASTNAME_INDEX = 2
BET_USER_DOCUMENT_INDEX = 3
BET_USER_BIRTH_INDEX = 4
BET_NUMBER_INDEX = 5

TOTAL_NUMBER_OF_CLIENTS = int(os.getenv('TOTAL_NUMBER_OF_CLIENTS'))


class Server:
    def __init__(self, port, listen_backlog):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)

        # Initialize server state
        self._server_working = False
        self._total_bets_registered = 0
        self._active_agencies_skt = {str(i): None for i in range(1, TOTAL_NUMBER_OF_CLIENTS + 1)}

        # Initialize signal handlers
        # Declaration of the SIGTERM handler
        signal.signal(signal.SIGTERM, self.graceful_shutdown)

        # Declaration of the SIGINT handler
        signal.signal(signal.SIGINT, self.graceful_shutdown)

    def graceful_shutdown(self, signum, frame):
        # Closure of the server socket
        self._server_socket.close()
        # Closure of all the active agencies sockets
        for agency_skt in self._active_agencies_skt.values():
            if agency_skt is not None:
                agency_skt.close()
        # Log the shutdown action
        logging.info('action: graceful_shutdown | result: success | signal number: {}'.format(signum))

    def _ready_for_lottery(self):
        """
        Checks if the server is ready to perform the lottery

        The server is ready to perform the lottery if all the agencies submitted their bets
        For convenience this will be checked with having the open socket for each agency

        Returns:
        - bool: True if the server is ready to perform the lottery, False otherwise
        """

        return all([agency_skt is not None for agency_skt in self._active_agencies_skt.values()])

    def _perform_lottery(self):
        """
        Performs the lottery

        The server will perform the lottery by checking all the bets stored
        and will print the winners
        """
        winning_bets = [bet for bet in load_bets() if has_won(bet)]

        winners_per_agency = {str(i): 0 for i in range(1, TOTAL_NUMBER_OF_CLIENTS + 1)}

        for bet in winning_bets:
            winners_per_agency[str(bet.agency)] += 1

        for agency, winners_amount in winners_per_agency.items():
            if self._active_agencies_skt[agency] is not None:
                self._send_all_winners_data(self._active_agencies_skt[agency], str(winners_amount))
            else:
                logging.error(f"action: sorteo | result: fail | error: agency {agency} is not connected")

    def run(self):
        """
        Agency Server loop

        The server will listen and accept new connections from clients
        Then the bets reported will be stored
        """

        self._server_working = True

        try:
            while self._server_working:
                if (self._ready_for_lottery()):
                    logging.info('action: sorteo | result: success')
                    self._perform_lottery()
                    self._server_working = False

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

    def _send_all(self, skt, message):
        """
        Sends a message to a given socket with short write handling
        """

        message_byte_len = len(message)
        total_bytes_sent = skt.send(message)

        while total_bytes_sent < message_byte_len:
            total_bytes_sent += skt.send(message[total_bytes_sent:])

    def _send_all_winners_data(self, agency_skt, winners_in_agency):
        """
        Sends the amount of winners from a given agency to the client
        """

        encoded_message = bytes(f"{FIELD_DELIMITER.join([f'{winners_in_agency}'])}{END_OF_BET_DELIMITER}{END_OF_BATCH_DELIMITER}".encode('utf-8'))
        self._send_all(agency_skt, encoded_message)


    def _send_all_batch_confirmation_data(self, client_sock, bets_received):
        """
        Sends the confirmation message to the client to let them know the batch was received
        """

        encoded_message = bytes(f"{FIELD_DELIMITER.join([f'Success on receiving {bets_received} bets'])}{END_OF_BET_DELIMITER}{END_OF_BATCH_DELIMITER}".encode('utf-8'))
        self._send_all(client_sock, encoded_message)

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            bets_received = self._recv_all_bets(client_sock)

            if AWAITING_RESULTS_MESSAGE in bets_received[0]:
                agency_id = bets_received[0][CLIENT_ID_INDEX]
                self._active_agencies_skt[agency_id] = client_sock
                return

            store_bets([self._build_bet_object(bet) for bet in bets_received])

            bets_stored = len(bets_received)
            self._total_bets_registered += bets_stored

            logging.info(f'action: apuesta_recibida | result: success | cantidad: {bets_stored}')

            self._send_all_batch_confirmation_data(client_sock, bets_stored)

            client_sock.close()

        except Exception as e:
            logging.error(f"action: apuesta_recibida | result: fail | cantidad: {self._total_bets_registered} | error: {e}")
            client_sock.close()

        logging.info(f'action: apuestas_recibidas | result: success | cantidad: {self._total_bets_registered}')

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

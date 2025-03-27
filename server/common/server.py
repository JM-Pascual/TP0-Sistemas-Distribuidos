import os
import signal
import socket
import logging
import threading

from .utils import Bet
from .bets_monitor import BetsMonitor

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
        self._client_threads = []

        # Initialize the monitor for the bets from the agencies
        self.bets_monitor = BetsMonitor(TOTAL_NUMBER_OF_CLIENTS)

        # Initialize signal handlers
        # Declaration of the SIGTERM handler
        signal.signal(signal.SIGTERM, self.graceful_shutdown)

        # Declaration of the SIGINT handler
        signal.signal(signal.SIGINT, self.graceful_shutdown)

    def _free_clients_resources(self):
        """
        Calls the public interface for freeing the clients resources allocated by the monitor
        """
        self.bets_monitor.free_clients_resources()
        self._reap_clients()

    def _free_resources_on_shutdown(self):
        """
        Calls the public interface for freeing the resources allocated by the monitor on shutdown
        This differs from the free_clients_resources method as it also aims to unlock all the threads in the program
        for successful complete shutdown
        """
        self.bets_monitor.shutdown()

    def _reap_clients(self):
        """
        Reaps the client threads that have finished and removes them from the list
        """
        for client_thread in self._client_threads:
            if not client_thread.is_alive():
                client_thread.join()
        self._client_threads = [client_thread for client_thread in self._client_threads if client_thread.is_alive()]

    def graceful_shutdown(self, signum, frame):
        # Closure of all the active agencies sockets
        self._free_resources_on_shutdown()
        self._server_working = False
        # Closure of the server socket
        self._server_socket.shutdown(socket.SHUT_RDWR)
        self._server_socket.close()
        # Log the shutdown action
        logging.info('action: graceful_shutdown | result: success | signal number: {}'.format(signum))

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

    def send_winners_data(self, agency_skt, winners_in_agency):
        """
        Provides a public interface to send the winners data to the agencies
        Allows for sync in monitor but communication logic in server
        """

        encoded_message = bytes(f"{FIELD_DELIMITER.join([f'{winners_in_agency}'])}{END_OF_BET_DELIMITER}{END_OF_BATCH_DELIMITER}".encode('utf-8'))
        self._send_all(agency_skt, encoded_message)


    def _send_all_batch_confirmation_data(self, client_sock, bets_received):
        """
        Sends the confirmation message to the client to let them know the batch was received
        """

        encoded_message = bytes(f"{FIELD_DELIMITER.join([f'Success on receiving {bets_received} bets'])}{END_OF_BET_DELIMITER}{END_OF_BATCH_DELIMITER}".encode('utf-8'))
        self._send_all(client_sock, encoded_message)

    def wait_for_lottery_time(self):
        """
        Calls the public interface for waiting for the lottery time
        """
        while self._server_working:
            self.bets_monitor.wait_for_lottery_time(self.send_winners_data)

            # It's possible that the server was shutdown while waiting for the lottery time and was unlocked by the shutdown
            # In that case, do not log the lottery success message
            if self._server_working:
                logging.info('action: sorteo | result: success')

            self._free_clients_resources()

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        client_is_registering_bets = True
        while client_is_registering_bets:
            try:
                bets_received = self._recv_all_bets(client_sock)

                if len(bets_received) == 0:
                    logging.error(f"action: apuesta_recibida | result: fail | error: no bets received")
                    client_sock.close()
                    client_is_registering_bets = False
                    continue

                agency_id = bets_received[0][CLIENT_ID_INDEX]

                if AWAITING_RESULTS_MESSAGE in bets_received[0]:
                    self.bets_monitor.notify_agency_awaiting_lottery(agency_id)
                    client_is_registering_bets = False
                    continue

                self.bets_monitor.store_agency_bets(agency_id, client_sock, [self._build_bet_object(bet) for bet in bets_received])

                bets_stored = len(bets_received)

                logging.info(f'action: apuesta_recibida | result: success | cantidad: {bets_stored}')

                self._send_all_batch_confirmation_data(client_sock, bets_stored)

            except Exception as e:
                logging.error(f"action: apuesta_recibida | result: fail | cantidad: {self.bets_monitor.get_total_bets_registered()} | error: {e}")
                client_sock.close()

        logging.info(f'action: apuestas_recibidas | result: success | cantidad: {self.bets_monitor.get_total_bets_registered()}')

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()

        # It's a good practice to reap the zombie/dead threads after accepting a new connection
        self._reap_clients()

        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c

    def run(self):
        """
        Agency Server loop

        The server will listen and accept new connections from clients
        Then the bets reported will be stored
        """

        self._server_working = True

        # Start the thread that will wait for the lottery time
        lottery_thread = threading.Thread(target=self.wait_for_lottery_time)
        lottery_thread.start()

        # The thread running the run method will be responsible for accepting new connections
        # The "Accept thead" will be responsible for handling the client connections starting the client threads
        try:
            while self._server_working:
                client_sock = self.__accept_new_connection()
                client_thread = threading.Thread(target=self.__handle_client_connection, args=(client_sock,))
                client_thread.start()
                self._client_threads.append(client_thread)
        except OSError as e:
            if self._server_working:
                logging.error(f"action: accepting new connections | result: fail | error: {e}")
                self._free_resources_on_shutdown()
                self._server_working = False
                self._server_socket.shutdown(socket.SHUT_RDWR)
                self._server_socket.close()
        finally:
            for thread in self._client_threads:
                thread.join()
                logging.info(f"action: client_thread | result: success | thread_id: {thread.ident}")
            self._client_threads = []
            lottery_thread.join()

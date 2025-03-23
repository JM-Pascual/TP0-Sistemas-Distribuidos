import signal
import socket
import logging

from .utils import Bet, store_bets

MAX_RECV_BUFFER_SIZE = 1024
FIELD_DELIMITER = "#"
END_OF_MESSAGE_DELIMITER = "\n"

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

        # Seteo del handler para la señal de SIGTERM
        signal.signal(signal.SIGTERM, self.graceful_shutdown)

        # Seteo del handler para la señal de SIGINT
        signal.signal(signal.SIGINT, self.graceful_shutdown)

    def graceful_shutdown(self, signum, frame):
        # Cierre del socket del servidor
        self._server_socket.close()
        # Loggeo de la acción
        logging.info('action: graceful_shutdown | result: success | signal number: {}'.format(signum))

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        # TODO: Modify this program to handle signal to graceful shutdown

        self._server_working = True

        try:
            while self._server_working:
                client_sock = self.__accept_new_connection()
                self.__handle_client_connection(client_sock)
        except OSError as e:
            logging.error(f"action: accepting new connections | result: fail | error: {e}")

    def _recv_all_bet_data(self, client_sock):
        raw_bet_data = bytes(client_sock.recv(MAX_RECV_BUFFER_SIZE))

        while raw_bet_data[-1] != ord(END_OF_MESSAGE_DELIMITER):
            raw_bet_data += bytes(client_sock.recv(MAX_RECV_BUFFER_SIZE))

        return raw_bet_data

    def _decode_submitted_bet(self, msg):
        """
        Decodes the message received from the client

        The message received from the client is a string with the following format:
        "first_name#last_name#document#birthdate#bet_number\n"

        "\n" beign the end of message delimiter.

        This function receives the message and returns a dictionary with the following keys:
        - first_name: str
        - last_name: str
        - document: str
        - birthdate: str
        - number: str
        """

        return msg.rstrip(END_OF_MESSAGE_DELIMITER).split(FIELD_DELIMITER)

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

    def _send_all_bet_confirmation_data(self, client_sock, decoded_bet_message):
        """
        Sends the confirmation message to the client by echoing the received message
        """

        encoded_message = bytes(f"{FIELD_DELIMITER.join(decoded_bet_message)}{END_OF_MESSAGE_DELIMITER}".encode('utf-8'))
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
            # TODO: Modify the receive to avoid short-reads
            msg = self._recv_all_bet_data(client_sock)

            decoded_bet_message = self._decode_submitted_bet(msg.decode('utf-8').rstrip(END_OF_MESSAGE_DELIMITER))

            store_bets([self._build_bet_object(decoded_bet_message)])

            logging.info(f'action: apuesta_almacenada | result: success | dni: {decoded_bet_message[BET_USER_DOCUMENT_INDEX]} | numero: {decoded_bet_message[BET_NUMBER_INDEX]}')

            # TODO: Modify the send to avoid short-writes

            self._send_all_bet_confirmation_data(client_sock, decoded_bet_message)

        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
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

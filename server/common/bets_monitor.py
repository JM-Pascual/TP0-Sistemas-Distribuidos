import threading
import logging
import socket
from .utils import Bet, store_bets, load_bets, has_won

class BetsMonitor:
    def __init__(self, clients_amount, lock = threading.Lock()):
        """
        initializes the _lock, threading.Lock() is used by default
        """
        self._clients_amount = clients_amount
        self._active_agencies_skt = {str(i): (False, None) for i in range(1, clients_amount + 1)}
        self._total_bets_registered = 0
        self._lock = lock
        self._lottery_time = threading.Condition(self._lock)

    def get_total_bets_registered(self):
        """
        returns the total bets registered in the storage file
        Blocking call, as it acquires the lock before returning the value to ensure consistency
        """
        with self._lock:
            return self._total_bets_registered

    def _ready_for_lottery(self):
        """
        Checks if the server is ready to perform the lottery

        The server is ready to perform the lottery if all the agencies submitted their bets
        For convenience this will be checked with having the open socket for each agency

        NON BLOCKING CALL, this should only be called from inside a thread that has acquired the lock with store_agency_bets

        Returns:
        - bool: True if the server is ready to perform the lottery, False otherwise
        """

        return all(agency_skt[0] for agency_skt in self._active_agencies_skt.values())

    def store_agency_bets(self, client_id, client_skt, bets: list[Bet]):
        """
        stores the bet in the storage file
        THIS IS A BLOCKING CALL, as it acquires the lock before storing the bet
        For simplicity in the exercise modeling, It'll be assumed that the clients submit all bets at once
        Meaning --> One call to store_bets per client
        """
        with self._lock:
            store_bets(bets)
            self._total_bets_registered += len(bets)
            self._active_agencies_skt[client_id] = (self._active_agencies_skt[client_id][0], client_skt)

    def notify_agency_awaiting_lottery(self, client_id):
        """
        Notifies that an agency is expecting the lottery
        """
        with self._lock:
            self._active_agencies_skt[client_id] = (True, self._active_agencies_skt[client_id][1])
            if self._ready_for_lottery():
                self._lottery_time.notify_all()

    def _perform_lottery(self, send_data_func):
        """
        Performs the lottery

        Should receive a function that encodes the data to be sent to the clients
        """

        if all(agency_skt[1] is None for agency_skt in self._active_agencies_skt.values()):
            # Probably the lock was released due to shutdown, so we should return and avoid using closed sockets
            return

        winning_bets = [bet for bet in load_bets() if has_won(bet)]

        winners_per_agency = {str(i): 0 for i in range(1, self._clients_amount + 1)}

        for bet in winning_bets:
            winners_per_agency[str(bet.agency)] += 1

        for agency, winners_amount in winners_per_agency.items():
            agency_skt = self._active_agencies_skt[agency][1]
            if agency_skt is not None:
                send_data_func(agency_skt, str(winners_amount))
            else:
                logging.error(f"action: sorteo | result: fail | error: agency {agency} is not connected")

    def wait_for_lottery_time(self, send_data_func):
        """
        waits for all clients to submit their bets
        """
        with self._lock:
            self._lottery_time.wait()
            self._perform_lottery(send_data_func)

    def free_clients_resources(self):
        """
        closes all the active agencies sockets
        """
        for agency_data in self._active_agencies_skt.values():
            agency_skt = agency_data[1]
            if agency_skt is not None:
                # Shutdown the socket in case the client is blocked in a recv call
                agency_skt.shutdown(socket.SHUT_RDWR)
                # Close the socket
                agency_skt.close()

        self._active_agencies_skt = {str(i): (False, None) for i in range(1, self._clients_amount + 1)}

    def shutdown(self):
        """
        closes all the active agencies sockets and releases the lock
        """
        self.free_clients_resources()
        # Calls notify to unlock threads that may be locked waiting for the lottery conditional variable
        with self._lock:
            self._lottery_time.notify_all()

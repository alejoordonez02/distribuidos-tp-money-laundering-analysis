import logging
import os
import time
from multiprocessing import Process, Queue
from uuid import UUID

from parser import AccountParser, TransactionParser

from common.comms.connection import Connection
from common.comms.messages import EOF, Accounts, Hello, HelloAck, Response, Transactions
from common.graceful_shutdown import setup_graceful_shutdown

NRESPONSES = int(os.getenv("NRESPONSES", "1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))
# Number of parser processes. Parsing (split + strptime + float) is CPU-bound and
# single-threaded per process, so we fan it out across cores; a single sender
# loop drains the shared queue and writes to the one gateway connection.
N_WORKERS = int(os.getenv("CLIENT_WORKERS", "4"))
_QUEUE_MAXSIZE = 256  # bounds in-flight batches → backpressure → bounded RAM
_DONE = b""  # per-worker end-of-range sentinel on the queue


def _transactions_worker(
    path: str, start: int, end: int, client_id: UUID, queue: "Queue"
) -> None:
    """Parse the byte range [start, end) of the transactions CSV and push each
    serialized ``Transactions`` batch onto the queue. Owns every line whose START
    offset falls in the range (Hadoop-style input split): seek to start, drop the
    first (partial/header) line, then read until a line starts at/after end."""
    parser = TransactionParser()
    batch = []
    with open(path, "rb") as f:
        f.seek(start)
        f.readline()  # skip header (worker 0) or the partial line owned by prev worker
        while f.tell() < end:
            line = f.readline()
            if not line:
                break
            batch.append(parser.parse(line.decode("utf-8")))
            if len(batch) >= BATCH_SIZE:
                queue.put(Transactions(client_id, batch).serialize())
                batch = []
    if batch:
        queue.put(Transactions(client_id, batch).serialize())
    queue.put(_DONE)


class Client:
    def __init__(
        self,
        conn: Connection,
        transactions_path: str,
        accounts_path: str,
        responses_path: str,
        n_workers: int = N_WORKERS,
    ):
        self.conn = conn
        self.client_id: UUID  # assigned by the gateway during the handshake
        self.transactions_path = transactions_path
        self.accounts_path = accounts_path
        self.responses_path = responses_path
        self.n_workers = max(1, n_workers)

    def start(self):
        setup_graceful_shutdown(self.stop)
        # TODO: esto lo dejo acá porque me trabé haciendo q se ejecute bien el script de healthcheck
        time.sleep(10)
        self._run()

    def stop(self):
        try:
            self.conn.close()
        except OSError as e:
            logging.error("!!! UNHANDLED OSError in client stop: %s", e, exc_info=True)

    def _run(self):
        # Handshake: ask the gateway for an id; it mints one and returns it in a
        # HelloAck. We stamp that gateway-assigned id on every message — and only
        # after receiving it can we spawn the workers, since they need it.
        self.conn.send(Hello().serialize())
        self.client_id = HelloAck.deserialize(self.conn.recv()).client_id

        count = self._send_transactions_parallel()
        self.conn.send(EOF(self.client_id, expected_count=count).serialize())
        logging.info("sent transactions eof to server (%d batches)", count)

        acc_count = self._send_accounts_batched()
        self.conn.send(EOF(self.client_id, expected_count=acc_count).serialize())
        logging.info("sent accounts eof to server (%d batches)", acc_count)

        logging.info("waiting for server responses")
        self._receive_and_write_responses()
        logging.info("received server responses. Bye")

        self.conn.close()

    def _send_transactions_parallel(self) -> int:
        """Fan parsing out to N worker processes over disjoint byte ranges; a
        single loop here drains the queue and sends each batch over the one
        connection. Order doesn't matter (the pipeline aggregates per client_id),
        and the EOF is only sent after every worker has finished — so no batch
        can arrive after it."""
        size = os.path.getsize(self.transactions_path)
        n = self.n_workers
        bounds = [(i * size // n, (i + 1) * size // n) for i in range(n)]

        queue: "Queue" = Queue(maxsize=_QUEUE_MAXSIZE)
        procs = [
            Process(
                target=_transactions_worker,
                args=(self.transactions_path, s, e, self.client_id, queue),
                daemon=True,
            )
            for s, e in bounds
        ]
        for p in procs:
            p.start()

        count = 0
        finished = 0
        while finished < n:
            item = queue.get()
            if item == _DONE:
                finished += 1
            else:
                self.conn.send(item)  # raw serialized Transactions bytes
                count += 1

        for p in procs:
            p.join()
        return count

    def _send_accounts_batched(self) -> int:
        """Accounts are a much smaller file, sent serially (text mode)."""
        parser = AccountParser()
        count = 0
        batch = []
        with open(self.accounts_path, "r") as f:
            f.readline()  # skip header
            while line := f.readline():
                batch.append(parser.parse(line))
                if len(batch) >= BATCH_SIZE:
                    self.conn.send(Accounts(self.client_id, batch).serialize())
                    batch = []
                    count += 1
            if batch:
                self.conn.send(Accounts(self.client_id, batch).serialize())
                count += 1
        return count

    def _receive_and_write_responses(self):
        # Each UC's result may arrive as several Response chunks (UC1/UC3 are
        # chunked to stay under the broker's max_message_size); a chunk with
        # last=True marks a completed UC. Count NRESPONSES completed UCs.
        completed = 0
        with open(self.responses_path, "w") as file:
            while completed < NRESPONSES:
                response = Response.deserialize(self.conn.recv())
                file.write(response.body)  # type: ignore[reportAttributeAccessIssue]
                if response.last:  # type: ignore[reportAttributeAccessIssue]
                    completed += 1
                    logging.info(f"received server response ({completed}/{NRESPONSES})")

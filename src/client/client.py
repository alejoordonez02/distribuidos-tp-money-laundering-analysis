import logging
import os
import shutil
import tempfile
import time
from multiprocessing import Process, Queue
from typing import TextIO
from uuid import UUID

from parser import AccountParser, TransactionParser

from common.comms.transport import Connection
from common.comms.messages import EOF, Accounts, Hello, HelloAck, Response, Transactions
from common.fault_injection import maybe_crash
from common.graceful_shutdown import setup_graceful_shutdown

NRESPONSES = int(os.getenv("NRESPONSES", "1"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "2000"))
N_WORKERS = int(os.getenv("CLIENT_WORKERS", "4"))
_QUEUE_MAXSIZE = 256
_DONE = b""


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
        f.readline()
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
        self.client_id: UUID
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
        self.conn.send(Hello().serialize())
        HelloAck.deserialize(self.conn.recv())
        # placeholder id; the gateway stamps the real one onto every forwarded message
        self.client_id = UUID(int=0)

        count = self._send_transactions_parallel()
        self.conn.send(EOF(self.client_id, expected_count=count).serialize())
        logging.info("sent transactions eof to server (%d batches)", count)

        acc_count = self._send_accounts_batched()
        self.conn.send(EOF(self.client_id, expected_count=acc_count).serialize())
        logging.info("sent accounts eof to server (%d batches)", acc_count)
        maybe_crash("client_after_eof")

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
                self.conn.send(item)
                count += 1
                maybe_crash("client_mid_transactions")

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
                    maybe_crash("client_mid_accounts")
            if batch:
                self.conn.send(Accounts(self.client_id, batch).serialize())
                count += 1
                # also fires when accounts fit in one sub-BATCH_SIZE batch (small datasets)
                maybe_crash("client_mid_accounts")
        return count

    def _receive_and_write_responses(self):
        handles: dict[int, TextIO] = {}
        paths: dict[int, str] = {}
        completed: set[int] = set()
        while len(completed) < NRESPONSES:
            response = Response.deserialize(self.conn.recv())
            uc_id = response.uc_id  # type: ignore[reportAttributeAccessIssue]
            handle = handles.get(uc_id)
            if not handle:
                fd, path = tempfile.mkstemp(prefix=f"resp_{uc_id}_", suffix=".txt")
                handle = os.fdopen(fd, "w")
                handles[uc_id] = handle
                paths[uc_id] = path
            handle.write(response.body)  # type: ignore[reportAttributeAccessIssue]
            if response.last:  # type: ignore[reportAttributeAccessIssue]
                completed.add(uc_id)
                logging.info("received UC %d (%d/%d)", uc_id, len(completed), NRESPONSES)

        with open(self.responses_path, "w") as out:
            for uc_id in sorted(paths):
                handles[uc_id].close()
                with open(paths[uc_id], "r") as f:
                    shutil.copyfileobj(f, out)
                os.unlink(paths[uc_id])

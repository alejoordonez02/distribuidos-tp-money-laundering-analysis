import logging
import os
import shutil
import tempfile
import time
from multiprocessing import Event, Process, Queue
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
_QUEUE_TIMEOUT = 10
_DONE = b""


def _transactions_worker(
    path: str, start: int, end: int, client_id: UUID, queue: "Queue", shutdown
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
            if shutdown.is_set():
                queue.cancel_join_thread()
                return
            line = f.readline()
            if not line:
                break
            batch.append(parser.parse(line.decode("utf-8")))
            if len(batch) >= BATCH_SIZE:
                queue.put(Transactions(client_id, batch).serialize())
                batch = []
    if shutdown.is_set():
        queue.cancel_join_thread()
        return
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
        self.shutdown_processes = Event()
        self.shutdown_main = Event()
        self.queue = None # type: ignore
        self.procs = []


    def start(self):
        setup_graceful_shutdown(self.stop)
        # TODO: esto lo dejo acá porque me trabé haciendo q se ejecute bien el script de healthcheck
        time.sleep(10)
        self._run()

    def stop(self):
        self.shutdown_processes.set()
        for p in self.procs:
            p.join()
            logging.info("Process joined")
        
        self.shutdown_main.set()
        
        if self.conn is not None:
            self.conn.close()
            logging.info("Socked closed")
        

        if self.queue is not None:
            self.queue.close()
            logging.info("Queue is closed")
        
    def _run(self):
        try:
            self.conn.send(Hello().serialize())
            HelloAck.deserialize(self.conn.recv())
        except:
            logging.info("Connection ended")
            return
        # placeholder id; the gateway stamps the real one onto every forwarded message
        self.client_id = UUID(int=0)

        count = self._send_transactions_parallel()
        if self.shutdown_main.is_set():
            return
        try:
            self.conn.send(EOF(self.client_id, expected_count=count).serialize())
            logging.info("sent transactions eof to server (%d batches)", count)
        except:
            logging.info("Connection ended")
            return

        acc_count = self._send_accounts_batched()
        if self.shutdown_main.is_set():
            return
        try:
            self.conn.send(EOF(self.client_id, expected_count=acc_count).serialize())
            logging.warning("sent accounts eof to server (%d batches)", acc_count)
        except:
            logging.info("Connection ended")
            return
        maybe_crash("client_after_eof")

        logging.warning("waiting for server responses")
        self._receive_and_write_responses()
        if self.shutdown_main.is_set():
            return
        logging.warning("received server responses. Bye")

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

        self.queue: "Queue" = Queue(maxsize=_QUEUE_MAXSIZE)
        self.procs = [
            Process(
                target=_transactions_worker,
                args=(self.transactions_path, s, e, self.client_id, self.queue, self.shutdown_processes),
                daemon=True,
            )
            for s, e in bounds
        ]
        for p in self.procs:
            p.start()

        count = 0
        finished = 0
        while finished < n and not self.shutdown_processes.is_set():    #DONT KNOW WHAT TO DO WITH THIS ONE
            try:
                item = self.queue.get(block=True, timeout=_QUEUE_TIMEOUT)
            except:
                logging.warning("Leaving Queue, timeout reached")
                return count
            if item == _DONE:
                finished += 1
            else:
                try:
                    self.conn.send(item)
                except:
                    logging.info("Connection ended")
                    return count                        ## Maybe raise error
                count += 1
                maybe_crash("client_mid_transactions")

        if not self.shutdown_processes.is_set():                        #DONT KNOW WHAT TO DO WITH THIS ONE
            for p in self.procs:
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
                    try:
                        self.conn.send(Accounts(self.client_id, batch).serialize())
                    except:
                        logging.info("Connection ended")
                        return count                                        ## Maybe raise error
                    batch = []
                    count += 1
                    maybe_crash("client_mid_accounts")
            if batch:
                try:
                    self.conn.send(Accounts(self.client_id, batch).serialize())
                except:
                    logging.info("Connection ended")
                    return count
                count += 1
                # also fires when accounts fit in one sub-BATCH_SIZE batch (small datasets)
                maybe_crash("client_mid_accounts")
        return count

    def _receive_and_write_responses(self):
        handles: dict[int, TextIO] = {}
        paths: dict[int, str] = {}
        completed: set[int] = set()
        while len(completed) < NRESPONSES:
            try:
                response = Response.deserialize(self.conn.recv())
            except:
                logging.info("Connection ended")
                return
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

from collections import defaultdict
import json
import os
import pathlib
import tempfile
from typing import Iterable
from uuid import UUID

from common.comms.messages import PathMsg
from common.comms.messages.graph_src.path import Path

from .aggregate_fn import AggregateFn

MAX_AMOUNT = 100
SHARDING_FILES = 100


def sharding_hash(path: Path, client_id: UUID) -> int:
    return hash(f"{client_id:}_{str(path)}") % SHARDING_FILES
    

def _serialize(path: Path, amount: int) -> str:
    return json.dumps([str(path), str(amount)])

def _deserialize(line: str) -> tuple[Path, int]:
    fields = json.loads(line)
    return Path.from_string(fields[0]), int(fields[1])
    

class UC4AggregatePaths(AggregateFn):
    def __init__(self):
        self._paths: dict[UUID, dict[Path, int]] = {}
        self._files: dict[UUID, dict[int, pathlib.Path]] = {}

    def _file_for(self, path: Path, client_id: UUID) -> pathlib.Path:
        hash = sharding_hash(path, client_id)
        if hash not in self._files[client_id]:
            fd, file_path = tempfile.mkstemp(prefix=f"UC4:{hash}", suffix=".jsonl")
            os.close(fd)
            self._files[client_id][hash] = pathlib.Path(file_path)
        return self._files[client_id][hash]


    def aggregate(self, msg: PathMsg):  # type: ignore[reportIncompatibleMethodOverride]
        if msg.client_id not in self._paths:
            self._paths[msg.client_id] = {}
        
        paths = self._paths[msg.client_id]
        paths[msg.path] = paths.get(msg.path, 0) + 1
        
        if len(paths) >= MAX_AMOUNT:
            self.downstream(msg.client_id)
        
    def get_result(self, client_id: UUID) -> Iterable[PathMsg]:
        for _, file in self._files[client_id].items():
            paths: dict[Path, int] = defaultdict()
            with open(file, "r") as f:
                for line in f:
                    path, amount = _deserialize(line)
                    paths[path] += amount
            file.unlink()
            for path, amount in paths.items():
                yield PathMsg(client_id, path, amount)
        self._files.pop(client_id)
    
    
    def downstream(self, client_id: UUID):
        paths = self._paths[client_id]
        # Aca si necesitamos una optimización podemos leer todo paths, agrupar los del mismo hashing
        # y hacer todo en una sola escritura de archivo
        for p, amount in paths.items():
            file = self._file_for(p, client_id)
            with open(file, "a") as f:
                f.write(_serialize(p, amount) + "\n")

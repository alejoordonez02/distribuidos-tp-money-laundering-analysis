from collections import defaultdict
import json
import logging
import os
import pathlib
import tempfile
from typing import Iterable
from uuid import UUID

from common.comms.messages import PathMsg
from common.comms.messages.graph_src.path import Path
from common.comms.messages.message import MessageJSONEncoder
from common.comms.messages.path_count import PathCounts

from .aggregate_fn import AggregateFn

MAX_AMOUNT = 10000
SHARDING_FILES = 1000


def sharding_hash(path: Path, client_id: UUID) -> int:
    return hash(f"{client_id:}_{str(path)}") % SHARDING_FILES
    

def _serialize(p: PathMsg) -> str:
    return json.dumps(p._fields(), cls=MessageJSONEncoder)

def _deserialize(line: str) -> tuple[Path, int]:
    p = PathMsg._from_fields(json.loads(line))
    return p.path, p.counts
    

class UC4AggregatePaths(AggregateFn):
    def __init__(self):
        self._paths: dict[UUID, dict[Path, int]] = {}
        self._files: dict[UUID, dict[int, pathlib.Path]] = {}

    def _file_for(self, path: Path, client_id: UUID) -> pathlib.Path:
        hash = sharding_hash(path, client_id)
        if client_id not in self._files:
            self._files[client_id] = {}
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
        
    def get_result(self, client_id: UUID) -> Iterable[PathCounts]:
        self.downstream(client_id)
        for _, file in self._files[client_id].items():
            paths: dict[Path, int] = defaultdict()
            with open(file, "r") as f:
                for line in f:
                    path, amount = _deserialize(line)
                    if path not in paths:
                        paths[path] = 0
                    paths[path] += amount
            file.unlink()
            yield PathCounts(client_id, paths)
        self._files.pop(client_id)
        
    
    
    def downstream(self, client_id: UUID):
        logging.info("Downstreaming")
        paths = self._paths[client_id]
        # Aca si necesitamos una optimización podemos leer todo paths, agrupar los del mismo hashing
        # y hacer todo en una sola escritura de archivo
        for shard in range(SHARDING_FILES):
            for p, amount in paths.items():
                if shard != sharding_hash(p, client_id):
                    continue
                file = self._file_for(p, client_id)
                with open(file, "a") as f:
                    f.write(_serialize(PathMsg(client_id, p, amount)) + "\n")

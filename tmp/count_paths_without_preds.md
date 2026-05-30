esto que estaba en compute graph me llama la atención...
```py
if preds.get(node) and succs.get(node)
```

no se puede hacer esto? acá no uso los preds...
```py
one_length_paths: dict[Node, set[Path]] = {}  # clave b, valor a->(b->)c
for a, a_succs in succs.items():
    for b in a_succs:
        b_succs = succs.get(b, [])

        for c in b_succs:
            if b not in one_length_paths:
                one_length_paths[b] = set()

            path = Path(a, c)
            one_length_paths[b].add(path)

counts: dict[Path, int] = {}
for _, paths in one_length_paths.items():
    for path in paths:
        if path not in counts:
            counts[path] = 0

        counts[path] += 1

result = PathCounts(client_id, counts)
```

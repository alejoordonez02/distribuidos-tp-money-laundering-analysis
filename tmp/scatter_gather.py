
transactions: list[tuple[str, str]] = []

class Node:
    def __init__(self, id2: str, predecessors: str, successors: str):
        self.id2 = id2
        self.predecessors = predecessors
        self.successors = successors

nodes: dict[str, tuple[set[str], set[str]]] = {}
"""
The graph of nodes
dict[key, tuple[set[key], set[key]]]
"""

# compute graph
for origin, destination in transactions:
    nodes[origin] = (set(), set(destination))
    nodes[destination] = (set(origin), set())


# count one length paths
one_length_paths: dict[tuple[str,str], int] = {}

for n in nodes.values():
    for a in n[0]:
        for s in n[1]:
            one_length_paths[(a,s)] += 1

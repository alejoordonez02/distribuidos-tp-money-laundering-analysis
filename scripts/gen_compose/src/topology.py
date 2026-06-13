"""Single source of truth for the pipeline topology.

Set how many replicas (ring peers) each controller has here, and every gen_* script
derives the rest automatically:

  - a stage's ring size  = its own count below (`npeers`)
  - a stage's fan-out    = the count of the stage it feeds (`naffinity_downstream`);
                           a work-queue downstream (the join) is 0, not a ring.

So any combination stays consistent — change a number here and the whole compose
adapts, no hand-matching of producer/consumer counts. 1 is valid everywhere (a
single-peer ring still routes by exchange).

Recommended profile below is tuned for a constrained host (≈4 physical cores, tight
RAM): lean overall, with the extra replicas spent only on the real bottlenecks
(UC4 path enumeration, the UC3 merge that spills all of period B, the broadcast
default filter). Bump a number to give that stage more parallelism.
"""

# --- entry: broadcasts every transaction to all five use-case pipelines ---
DEFAULT_FILTERS = 2

# --- UC2: max-amount-by-bank joined with bank names ---
UC2_MAX_AMOUNT_GROUP_BYS = 1
UC2_MAX_AMOUNT_AGGREGATES = 1
UC2_BANK_NAMES_GROUP_BYS = 1
UC2_BANK_NAMES_AGGREGATES = 1
UC2_MERGES = 2

# --- UC3: average-by-format, then period-B filtered against it ---
UC3_GROUP_BYS = 1          # partial sums by payment format
UC3_AGGREGATES = 1         # the averages
UC3_MERGES = 2             # spills ALL of period B to disk — the classic bottleneck
UC3_FILTERS = 1

# --- UC4: transaction-graph path counting + high-degree prune ---
UC4_COMPUTE_GRAPHS = 2
UC4_AGGREGATE_GRAPHS = 1
UC4_DEGREE_COMPUTE_GRAPHS = 1
UC4_DEGREE_AGGREGATES = 1
UC4_PRUNES = 2
UC4_COUNT_PATHS = 2        # combinatorial path enumeration — the heaviest stage
UC4_PATHS_AGGREGATES = 2

# --- UC5: USD-converted amount filter + count ---
UC5_CONVERTERS = 1
UC5_AMOUNT_FILTERS = 1
UC5_COUNT_GROUP_BYS = 1

# --- the join: partitioned by use-case, one inner list per container (not a ring).
# THIS is the single place that decides which UC's join handler runs where. UC1 and
# UC3 each spill every transaction, so each gets its own container; the lighter joins
# share the third. Add/remove a list or move a UC to repartition. ---
JOIN_PARTITION = [
    [1],        # join_0: UC1
    [3],        # join_1: UC3
    [2, 4, 5],  # join_2: the lighter joins
]

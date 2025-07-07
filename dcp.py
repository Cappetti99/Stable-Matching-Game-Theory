import networkx as nx
from models import Task, VM

def determine_critical_path(dag, vm_pool, ccr):
    ranks = {}
    avg_capacity = sum(vm.capacity for vm in vm_pool) / len(vm_pool)

    def rank(tid):
        if tid in ranks:
            return ranks[tid]
        task = dag.nodes[tid]['data']
        Wi = task.size / avg_capacity
        succs = list(dag.successors(tid))
        if not succs:
            ranks[tid] = Wi
        else:
            ranks[tid] = Wi + max(ccr + rank(s) for s in succs)
        return ranks[tid]

    for tid in dag.nodes:
        rank(tid)

    levels = {}
    for node in nx.topological_sort(dag):
        preds = list(dag.predecessors(node))
        levels[node] = 0 if not preds else max(levels[p] for p in preds) + 1

    max_per_level = {}
    for node, level in levels.items():
        if level not in max_per_level or ranks[node] > ranks[max_per_level[level]]:
            max_per_level[level] = node

    return list(max_per_level.values())
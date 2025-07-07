"""
Baseline implementation for the Stable‑Matching Cloud Workflow Scheduler
=============================================================================
This script now contains:
✓ Stable‑Matching pre‑scheduling (very basic)
✓ Critical Path Determination (DCP)
✓ Metric evaluation and matplotlib graph plotting (SLR, AVU, VF)
"""

from dataclasses import dataclass, field
import networkx as nx
import random
import matplotlib.pyplot as plt
import numpy as np

@dataclass
class Task:
    id: int
    size: float
    assigned_vm: int = None

@dataclass
class VM:
    id: int
    capacity: float
    available_time: float = 0.0
    waiting_tasks: list = field(default_factory=list)

def execution_time(task: Task, vm: VM) -> float:
    return task.size / vm.capacity

def stable_matching(dag: nx.DiGraph, vm_pool: list, ccr: float, critical_path=None):
    """
    Implements Algorithm 3: Stable Matching Game Theory (SMGT)
    """
    schedule = {}
    levels = {}  # task_id -> level

    # Costruzione livelli per ciascun task (basato su predecessori)
    for node in nx.topological_sort(dag):
        preds = list(dag.predecessors(node))
        levels[node] = 0 if not preds else 1 + max(levels[p] for p in preds)

    max_level = max(levels.values())
    level_dict = {l: [] for l in range(max_level + 1)}
    for task_id, level in levels.items():
        level_dict[level].append(task_id)

    # Soglia fissa per VM (es: massimo 2 task per VM)
    for vm in vm_pool:
        vm.waiting_tasks = []
        vm.threshold = 2

    def task_vm_preferences(task: Task):
        return sorted(vm_pool, key=lambda vm: execution_time(task, vm))

    def task_rank_in_vm(task: Task, vm: VM):
        return task_vm_preferences(task).index(vm)

    def worst_task(vm: VM):
        return max(vm.waiting_tasks, key=lambda t: task_rank_in_vm(t, vm))

    # Algoritmo SMGT
    for l in range(max_level + 1):
        task_ids = level_dict[l]

        # a. Assegna subito il task critico del livello alla VM più veloce
        if critical_path:
            crit_in_level = [tid for tid in task_ids if tid in critical_path]
            if crit_in_level:
                t_id = crit_in_level[0]
                task = dag.nodes[t_id]['data']
                best_vm = min(vm_pool, key=lambda vm: execution_time(task, vm))
                best_vm.waiting_tasks.append(task)
                schedule[t_id] = best_vm.id
                task_ids.remove(t_id)

        # b. Assegna gli altri task tramite preferenze e threshold
        task_queue = [(tid, dag.nodes[tid]['data']) for tid in task_ids]

        while task_queue:
            t_id, task = task_queue.pop(0)
            prefs = task_vm_preferences(task)

            for vm in prefs:
                if len(vm.waiting_tasks) < vm.threshold:
                    vm.waiting_tasks.append(task)
                    schedule[t_id] = vm.id
                    break
                else:
                    worst = worst_task(vm)
                    if task_rank_in_vm(task, vm) < task_rank_in_vm(worst, vm):
                        vm.waiting_tasks.remove(worst)
                        vm.waiting_tasks.append(task)
                        schedule[t_id] = vm.id
                        task_queue.append((worst.id, worst))
                        break

    return schedule

def determine_critical_path(dag: nx.DiGraph, vm_pool: list, ccr: float):
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
            max_rank = max(ccr + rank(s) for s in succs)  # c_ij = ccr
            ranks[tid] = Wi + max_rank
        return ranks[tid]
    for tid in dag.nodes:
        rank(tid)
    levels = {}
    for node in nx.topological_sort(dag):
        preds = list(dag.predecessors(node))
        level = 0 if not preds else max(levels[p] for p in preds) + 1
        levels[node] = level
    max_per_level = {}
    for node, level in levels.items():
        if level not in max_per_level or ranks[node] > ranks[max_per_level[level]]:
            max_per_level[level] = node
    return list(max_per_level.values())

def task_duplication_optimization(dag: nx.DiGraph, schedule: dict, vm_pool: list):
    print("[TODO] task_duplication_optimization: not yet implemented.")
    return schedule

def compute_metrics(dag, vm_pool, schedule, ccr):
    end_times = {vm.id: 0 for vm in vm_pool}
    task_times = {}
    for tid, vid in schedule.items():
        t = dag.nodes[tid]['data']
        vm = next(v for v in vm_pool if v.id == vid)
        st = end_times[vid]
        dur = execution_time(t, vm)
        end_times[vid] = st + dur
        task_times[tid] = dur
    makespan = max(end_times.values())
    cp = determine_critical_path(dag, vm_pool, ccr)
    denom = sum(dag.nodes[tid]['data'].size / max(v.capacity for v in vm_pool) for tid in cp)
    slr = makespan / denom
    busy = sum(end_times.values())
    avu = (busy / len(vm_pool)) / makespan
    ratios = []
    for tid, t in dag.nodes(data='data'):
        eet = t.size / max(v.capacity for v in vm_pool)
        aet = task_times[tid]
        ratios.append(eet / aet)
    vf = np.var(ratios)
    return slr, avu, vf

def schedule_sm_cptd(dag, vm_pool, ccr):
    critical_path = determine_critical_path(dag, vm_pool, ccr)
    pre_schedule = stable_matching(dag, vm_pool, ccr, critical_path)
    final_schedule = task_duplication_optimization(dag, pre_schedule, vm_pool)
    return final_schedule

if __name__ == "__main__":
    dag = nx.DiGraph()
    tasks = [Task(i, size=random.randint(10, 50)) for i in range(5)]
    for t in tasks:
        dag.add_node(t.id, data=t)
    dag.add_edges_from([(0, 2), (1, 2), (2, 3), (2, 4)])

    ccr_values = [0.5, 1.0, 2.0, 4.0]
    slr_list, avu_list, vf_list = [], [], []

    for ccr in ccr_values:
        vms = [VM(i, capacity=random.uniform(5, 10)) for i in range(3)]
        schedule = schedule_sm_cptd(dag, vms, ccr)
        slr, avu, vf = compute_metrics(dag, vms, schedule, ccr)
        slr_list.append(slr)
        avu_list.append(avu)
        vf_list.append(vf)

    plt.figure(figsize=(10, 6))
    plt.plot(ccr_values, slr_list, '-o', label='SLR')
    plt.plot(ccr_values, avu_list, '-s', label='AVU')
    plt.plot(ccr_values, vf_list, '-^', label='VF')
    plt.xlabel('CCR')
    plt.ylabel('Metric Value')
    plt.legend()
    plt.title('Performance SM‑CPTD vs CCR')
    plt.grid(True)
    plt.show()
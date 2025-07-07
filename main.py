"""main.py
Entry‑point script that wires together the modules.
Run `python main.py` to generate sample metrics and a plot.
"""
from __future__ import annotations
import random
import networkx as nx
import matplotlib.pyplot as plt

from models import Task, VM
from smgt import stable_matching
from dcp import determine_critical_path
from lotd import task_duplication_optimization
from metrics import compute_metrics


def build_demo_workflow() -> nx.DiGraph:
    dag = nx.DiGraph()
    tasks = [Task(i, size=random.randint(10, 50)) for i in range(5)]
    for t in tasks:
        dag.add_node(t.id, data=t)
    dag.add_edges_from([(0, 2), (1, 2), (2, 3), (2, 4)])
    return dag


def run_experiment():
    dag = build_demo_workflow()
    ccr_values = [0.5, 1.0, 2.0, 4.0]

    slr_list, avu_list, vf_list = [], [], []

    for ccr in ccr_values:
        vms = [VM(i, capacity=random.uniform(5, 10)) for i in range(3)]
        cp = determine_critical_path(dag, vms, ccr)
        pre = stable_matching(dag, vms, ccr, cp)
        sched = task_duplication_optimization(dag, pre, vms)
        slr, avu, vf = compute_metrics(dag, vms, sched, ccr)

        slr_list.append(slr)
        avu_list.append(avu)
        vf_list.append(vf)

    plt.figure(figsize=(10, 6))
    plt.plot(ccr_values, slr_list, "-o", label="SLR")
    plt.plot(ccr_values, avu_list, "-s", label="AVU")
    plt.plot(ccr_values, vf_list, "-^", label="VF")
    plt.xlabel("CCR")
    plt.ylabel("Metric Value")
    plt.title("SM‑CPTD Performance vs. CCR (demo)")
    plt.legend()
    plt.grid(True)
    plt.show()


if __name__ == "__main__":
    run_experiment()
import networkx as nx
from models import Task, VM, execution_time

def stable_matching(dag, vm_pool, ccr, critical_path=None):
    schedule = {}
    levels = {}
    for node in nx.topological_sort(dag):
        preds = list(dag.predecessors(node))
        levels[node] = 0 if not preds else 1 + max(levels[p] for p in preds)

    max_level = max(levels.values())
    level_dict = {l: [] for l in range(max_level + 1)}
    for tid, lvl in levels.items():
        level_dict[lvl].append(tid)

    for vm in vm_pool:
        vm.waiting_tasks = []
        vm.threshold = 2

    def task_vm_preferences(task):
        return sorted(vm_pool, key=lambda vm: execution_time(task, vm))

    def task_rank_in_vm(task, vm):
        return task_vm_preferences(task).index(vm)

    def worst_task(vm):
        return max(vm.waiting_tasks, key=lambda t: task_rank_in_vm(t, vm))

    for l in range(max_level + 1):
        task_ids = level_dict[l]
        if critical_path:
            crit = [tid for tid in task_ids if tid in critical_path]
            if crit:
                t_id = crit[0]
                task = dag.nodes[t_id]['data']
                best_vm = min(vm_pool, key=lambda vm: execution_time(task, vm))
                best_vm.waiting_tasks.append(task)
                schedule[t_id] = best_vm.id
                task_ids.remove(t_id)

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
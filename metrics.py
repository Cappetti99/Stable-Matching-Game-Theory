import numpy as np
from models import execution_time
from dcp import determine_critical_path

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
package com.example.scheduler.scheduler;

import com.example.scheduler.model.Task;
import com.example.scheduler.model.VM;
import com.example.scheduler.utils.WorkflowDAG;

import java.util.*;

public class SMGT {

    public static Map<Integer, Integer> schedule(
            WorkflowDAG dag,
            Map<Integer, Task> tasks,
            List<VM> vmPool,
            List<Integer> criticalPath
    ) {
        Map<Integer, Integer> schedule = new HashMap<>();
        Set<Integer> scheduledTasks = new HashSet<>();

        // Assegna i task del Critical Path prima
        for (int taskId : criticalPath) {
            Task task = tasks.get(taskId);
            if (task.assignedVM == null) {
                VM bestVM = findBestVM(vmPool, task);
                if (bestVM != null) {
                    bestVM.waitingTasks.add(task);
                    task.assignedVM = bestVM.id;
                    schedule.put(taskId, bestVM.id);
                    scheduledTasks.add(taskId);
                }
            }
        }

        // Assegna gli altri task in ordine topologico
        for (int taskId : dag.topologicalSort()) {
            if (!scheduledTasks.contains(taskId)) {
                Task task = tasks.get(taskId);
                if (task.assignedVM == null) {
                    VM bestVM = findBestVM(vmPool, task);
                    if (bestVM != null) {
                        bestVM.waitingTasks.add(task);
                        task.assignedVM = bestVM.id;
                        schedule.put(taskId, bestVM.id);
                    }
                }
            }
        }
        return schedule;
    }

    private static VM findBestVM(List<VM> vmPool, Task task) {
        return vmPool.stream()
                .min(Comparator.comparingDouble(vm -> task.size / vm.capacity))
                .orElse(null);
    }
}

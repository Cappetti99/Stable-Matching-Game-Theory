package com.example.scheduler.scheduler;

import com.example.scheduler.model.Task;
import com.example.scheduler.model.VM;
import com.example.scheduler.utils.WorkflowDAG;

import java.util.*;

public class DCP {

    public static List<Integer> determineCriticalPath(
            WorkflowDAG dag,
            Map<Integer, Task> tasks,
            List<VM> vmPool,
            double ccr
    ) {
        Map<Integer, Double> ranks = new HashMap<>();
        double avgCapacity = vmPool.stream().mapToDouble(vm -> vm.capacity).average().orElse(0.0);
        double avgTaskSize = tasks.values().stream().mapToDouble(task -> task.size).average().orElse(0.0);
        double threshold = avgCapacity / avgTaskSize * ccr;
        List<Integer> criticalPath = new ArrayList<>();
        for (int taskId : dag.getAllTasks()) {
            double rank = calculateRank(taskId, dag, tasks, threshold);
            ranks.put(taskId, rank);
        }
        List<Map.Entry<Integer, Double>> sortedTasks = new ArrayList<>(ranks.entrySet());
        sortedTasks.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));
        for (Map.Entry<Integer, Double> entry : sortedTasks) {
            criticalPath.add(entry.getKey());
        }      
        return criticalPath;
    }
    private static double calculateRank(int taskId, WorkflowDAG dag, Map<Integer, Task> tasks, double threshold) {
        double rank = 0.0;
        List<Integer> successors = dag.getSuccessors(taskId);
        for (int succ : successors) {
            if (tasks.get(succ).assignedVM == null) {
                rank += tasks.get(succ).size / threshold;
            }
        }
        return rank;
    }
}

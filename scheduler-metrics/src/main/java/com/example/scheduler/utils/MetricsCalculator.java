package com.example.scheduler.utils;

import com.example.scheduler.model.Task;
import com.example.scheduler.model.VM;

import java.util.*;

public class MetricsCalculator {

    public static class DAGNode {
        public Task task;
        public int id;

        public DAGNode(int id, Task task) {
            this.id = id;
            this.task = task;
        }
    }

    public static double executionTime(Task task, VM vm) {
        return task.size / vm.capacity;
    }

    public static List<Integer> determineCriticalPath(WorkflowDAG dag, List<VM> vmPool, double ccr) {
        // Stub da sostituire con implementazione reale
        return new ArrayList<>(dag.getAllTasks());
    }

    public static class Metrics {
        public double slr;
        public double avu;
        public double vf;

        public Metrics(double slr, double avu, double vf) {
            this.slr = slr;
            this.avu = avu;
            this.vf = vf;
        }
    }

    public static Metrics computeMetrics(WorkflowDAG dag, List<VM> vmPool, Map<Integer, Integer> schedule, double ccr, Map<Integer, Task> taskMap) {
        Map<Integer, Double> endTimes = new HashMap<>();
        Map<Integer, Double> taskDurations = new HashMap<>();

        for (VM vm : vmPool) {
            endTimes.put(vm.id, 0.0);
        }

        for (Map.Entry<Integer, Integer> entry : schedule.entrySet()) {
            int taskId = entry.getKey();
            int vmId = entry.getValue();

            Task task = taskMap.get(taskId);
            VM vm = vmPool.stream().filter(v -> v.id == vmId).findFirst().orElseThrow();
            double startTime = endTimes.get(vmId);
            double duration = executionTime(task, vm);

            endTimes.put(vmId, startTime + duration);
            taskDurations.put(taskId, duration);
        }

        double makespan = Collections.max(endTimes.values());

        List<Integer> criticalPath = determineCriticalPath(dag, vmPool, ccr);
        double maxCapacity = vmPool.stream().mapToDouble(vm -> vm.capacity).max().orElse(1.0);
        double denom = 0.0;

        for (int tid : criticalPath) {
            Task t = taskMap.get(tid);
            denom += t.size / maxCapacity;
        }

        double slr = makespan / denom;

        double totalBusyTime = endTimes.values().stream().mapToDouble(Double::doubleValue).sum();
        double avu = (totalBusyTime / vmPool.size()) / makespan;

        List<Double> ratios = new ArrayList<>();
        for (int tid : dag.getAllTasks()) {
            Task t = taskMap.get(tid);
            double eet = t.size / maxCapacity;
            double aet = taskDurations.getOrDefault(tid, 1.0); // fallback to 1.0 to avoid division by zero
            ratios.add(eet / aet);
        }

        double mean = ratios.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double variance = ratios.stream().mapToDouble(r -> Math.pow(r - mean, 2)).average().orElse(0.0);

        return new Metrics(slr, avu, variance);
    }
}
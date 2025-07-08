package com.example.scheduler.utils;

import com.example.scheduler.model.Task;
import com.example.scheduler.model.VM;

import java.util.*;

public class MetricsCalculator {

    public static class Metrics {
        public double slr;      // Schedule Length Ratio
        public double avu;      // Average VM Utilization
        public double vf;       // Variance Factor
        public double makespan; // Total execution time
        public double efficiency; // Additional metric
        
        public Metrics(double slr, double avu, double vf, double makespan) {
            this.slr = slr;
            this.avu = avu;
            this.vf = vf;
            this.makespan = makespan;
            this.efficiency = avu / slr; // Combined efficiency metric
        }
    }

    public static double executionTime(Task task, VM vm) {
        return task.size / vm.capacity;
    }

    public static Metrics computeMetrics(WorkflowDAG dag, List<VM> vmPool, 
                                       Map<Integer, Integer> schedule, double ccr, 
                                       Map<Integer, Task> taskMap) {
        
        // Calculate task execution times and VM loads
        Map<Integer, List<TaskExecution>> vmSchedules = new HashMap<>();
        Map<Integer, Double> taskStartTimes = new HashMap<>();
        Map<Integer, Double> taskEndTimes = new HashMap<>();
        
        // Initialize VM schedules
        for (VM vm : vmPool) {
            vmSchedules.put(vm.id, new ArrayList<>());
        }
        
        // Schedule tasks in topological order to respect dependencies
        List<Integer> topologicalOrder = dag.topologicalSort();
        
        for (int taskId : topologicalOrder) {
            if (!schedule.containsKey(taskId)) continue;
            
            Task task = taskMap.get(taskId);
            int vmId = schedule.get(taskId);
            VM vm = vmPool.stream().filter(v -> v.id == vmId).findFirst().orElse(null);
            
            if (vm == null) continue;
            
            // Calculate earliest start time considering dependencies
            double earliestStart = 0.0;
            
            // Check predecessor constraints
            for (int predId : dag.getPredecessors(taskId)) {
                if (taskEndTimes.containsKey(predId)) {
                    double commTime = 0.0;
                    // Add communication time if predecessor is on different VM
                    if (schedule.get(predId) != vmId) {
                        commTime = calculateCommunicationTime(predId, taskId, dag, taskMap, ccr);
                    }
                    earliestStart = Math.max(earliestStart, taskEndTimes.get(predId) + commTime);
                }
            }
            
            // Check VM availability
            List<TaskExecution> vmTasks = vmSchedules.get(vmId);
            double vmAvailable = vmTasks.isEmpty() ? 0.0 : 
                vmTasks.stream().mapToDouble(te -> te.endTime).max().orElse(0.0);
            
            double startTime = Math.max(earliestStart, vmAvailable);
            double execTime = executionTime(task, vm);
            double endTime = startTime + execTime;
            
            taskStartTimes.put(taskId, startTime);
            taskEndTimes.put(taskId, endTime);
            vmSchedules.get(vmId).add(new TaskExecution(taskId, startTime, endTime));
        }
        
        // Calculate makespan
        double makespan = taskEndTimes.values().stream()
                .mapToDouble(Double::doubleValue).max().orElse(0.0);
        
        // Calculate critical path length for SLR
        double criticalPathLength = calculateCriticalPathLength(dag, taskMap, vmPool, ccr);
        double slr = makespan / Math.max(criticalPathLength, 1.0);
        
        // Calculate Average VM Utilization (AVU)
        double totalUtilization = 0.0;
        for (VM vm : vmPool) {
            List<TaskExecution> vmTasks = vmSchedules.get(vm.id);
            double vmBusyTime = vmTasks.stream()
                    .mapToDouble(te -> te.endTime - te.startTime).sum();
            totalUtilization += vmBusyTime / makespan;
        }
        double avu = totalUtilization / vmPool.size();
        
        // Calculate Variance Factor (VF)
        double vf = calculateVarianceFactor(taskMap, vmPool, schedule, taskStartTimes, taskEndTimes);
        
        return new Metrics(slr, avu, vf, makespan);
    }
    
    private static double calculateCommunicationTime(int fromTask, int toTask, 
                                                   WorkflowDAG dag, Map<Integer, Task> taskMap, 
                                                   double ccr) {
        // Simple communication time model
        Task from = taskMap.get(fromTask);
        Task to = taskMap.get(toTask);
        
        // Communication time proportional to data size and CCR
        double dataSize = Math.min(from.size, to.size) * 0.1; // 10% of smaller task
        double avgCapacity = 10.0; // Simplified average capacity
        
        return (dataSize / avgCapacity) * ccr;
    }
    
    private static double calculateCriticalPathLength(WorkflowDAG dag, Map<Integer, Task> taskMap, 
                                                    List<VM> vmPool, double ccr) {
        // Calculate the length of the critical path
        Map<Integer, Double> taskRanks = new HashMap<>();
        double maxCapacity = vmPool.stream().mapToDouble(vm -> vm.capacity).max().orElse(1.0);
        
        // Calculate ranks in reverse topological order
        List<Integer> reverseTopo = new ArrayList<>(dag.topologicalSort());
        Collections.reverse(reverseTopo);
        
        for (int taskId : reverseTopo) {
            Task task = taskMap.get(taskId);
            double taskTime = task.size / maxCapacity;
            
            double maxSuccessorRank = 0.0;
            for (int succ : dag.getSuccessors(taskId)) {
                double commTime = calculateCommunicationTime(taskId, succ, dag, taskMap, ccr);
                maxSuccessorRank = Math.max(maxSuccessorRank, 
                    taskRanks.getOrDefault(succ, 0.0) + commTime);
            }
            
            taskRanks.put(taskId, taskTime + maxSuccessorRank);
        }
        
        return taskRanks.values().stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
    }
    
    private static double calculateVarianceFactor(Map<Integer, Task> taskMap, List<VM> vmPool, 
                                                Map<Integer, Integer> schedule,
                                                Map<Integer, Double> taskStartTimes,
                                                Map<Integer, Double> taskEndTimes) {
        
        List<Double> executionRatios = new ArrayList<>();
        double maxCapacity = vmPool.stream().mapToDouble(vm -> vm.capacity).max().orElse(1.0);
        
        for (Map.Entry<Integer, Task> entry : taskMap.entrySet()) {
            int taskId = entry.getKey();
            Task task = entry.getValue();
            
            if (schedule.containsKey(taskId) && taskStartTimes.containsKey(taskId) && 
                taskEndTimes.containsKey(taskId)) {
                
                double expectedTime = task.size / maxCapacity;
                double actualTime = taskEndTimes.get(taskId) - taskStartTimes.get(taskId);
                
                if (actualTime > 0) {
                    executionRatios.add(expectedTime / actualTime);
                }
            }
        }
        
        if (executionRatios.isEmpty()) return 0.0;
        
        double mean = executionRatios.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        double variance = executionRatios.stream()
                .mapToDouble(r -> Math.pow(r - mean, 2))
                .average().orElse(0.0);
        
        return variance;
    }
    
    private static class TaskExecution {
        int taskId;
        double startTime;
        double endTime;
        
        TaskExecution(int taskId, double startTime, double endTime) {
            this.taskId = taskId;
            this.startTime = startTime;
            this.endTime = endTime;
        }
    }
}
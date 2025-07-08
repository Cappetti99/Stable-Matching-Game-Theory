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
        // Calculate average computation and communication costs
        double avgCapacity = vmPool.stream().mapToDouble(vm -> vm.capacity).average().orElse(1.0);
        double avgTaskSize = tasks.values().stream().mapToDouble(task -> task.size).average().orElse(1.0);
        double avgComputationCost = avgTaskSize / avgCapacity;
        double avgCommunicationCost = avgComputationCost * ccr;
        
        // Calculate upward ranks for all tasks
        Map<Integer, Double> upwardRanks = calculateUpwardRanks(dag, tasks, avgCapacity, avgCommunicationCost);
        
        // Find the critical path starting from the task with highest upward rank
        int entryTask = upwardRanks.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(dag.topologicalSort().get(0));
        
        List<Integer> criticalPath = new ArrayList<>();
        Set<Integer> visited = new HashSet<>();
        
        // Trace the critical path
        traceCriticalPath(entryTask, dag, tasks, avgCapacity, avgCommunicationCost, 
                         upwardRanks, criticalPath, visited);
        
        return criticalPath;
    }
    
    private static Map<Integer, Double> calculateUpwardRanks(WorkflowDAG dag, 
                                                           Map<Integer, Task> tasks, 
                                                           double avgCapacity, 
                                                           double avgCommunicationCost) {
        Map<Integer, Double> upwardRanks = new HashMap<>();
        
        // Process tasks in reverse topological order
        List<Integer> reverseTopo = new ArrayList<>(dag.topologicalSort());
        Collections.reverse(reverseTopo);
        
        for (int taskId : reverseTopo) {
            Task task = tasks.get(taskId);
            double computationCost = task.size / avgCapacity;
            
            // Find maximum of successor ranks + communication cost
            double maxSuccessorRank = 0.0;
            List<Integer> successors = dag.getSuccessors(taskId);
            
            for (int successor : successors) {
                double successorRank = upwardRanks.getOrDefault(successor, 0.0);
                double communicationCost = avgCommunicationCost; // Simplified
                maxSuccessorRank = Math.max(maxSuccessorRank, successorRank + communicationCost);
            }
            
            upwardRanks.put(taskId, computationCost + maxSuccessorRank);
        }
        
        return upwardRanks;
    }
    
    private static void traceCriticalPath(int currentTask, WorkflowDAG dag, 
                                        Map<Integer, Task> tasks, double avgCapacity, 
                                        double avgCommunicationCost, 
                                        Map<Integer, Double> upwardRanks,
                                        List<Integer> criticalPath, Set<Integer> visited) {
        
        if (visited.contains(currentTask)) return;
        
        visited.add(currentTask);
        criticalPath.add(currentTask);
        
        // Find the successor on the critical path
        List<Integer> successors = dag.getSuccessors(currentTask);
        if (successors.isEmpty()) return;
        
        // Choose successor with maximum (rank + communication cost)
        int criticalSuccessor = -1;
        double maxCriticalCost = Double.NEGATIVE_INFINITY;
        
        for (int successor : successors) {
            double successorRank = upwardRanks.getOrDefault(successor, 0.0);
            double communicationCost = avgCommunicationCost;
            double totalCost = successorRank + communicationCost;
            
            if (totalCost > maxCriticalCost) {
                maxCriticalCost = totalCost;
                criticalSuccessor = successor;
            }
        }
        
        // Continue tracing the critical path
        if (criticalSuccessor != -1) {
            traceCriticalPath(criticalSuccessor, dag, tasks, avgCapacity, 
                            avgCommunicationCost, upwardRanks, criticalPath, visited);
        }
    }
    
    /**
     * Alternative method to get critical path by priority ranking
     */
    public static List<Integer> getCriticalPathByPriority(
            WorkflowDAG dag,
            Map<Integer, Task> tasks,
            List<VM> vmPool,
            double ccr
    ) {
        Map<Integer, Double> priorities = calculateTaskPriorities(dag, tasks, vmPool, ccr);
        
        // Sort tasks by priority (highest first)
        List<Map.Entry<Integer, Double>> sortedTasks = new ArrayList<>(priorities.entrySet());
        sortedTasks.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));
        
        return sortedTasks.stream()
                .map(Map.Entry::getKey)
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }
    
    private static Map<Integer, Double> calculateTaskPriorities(
            WorkflowDAG dag,
            Map<Integer, Task> tasks,
            List<VM> vmPool,
            double ccr
    ) {
        Map<Integer, Double> priorities = new HashMap<>();
        double avgCapacity = vmPool.stream().mapToDouble(vm -> vm.capacity).average().orElse(1.0);
        
        // Calculate bottom-level (b-level) for each task
        Map<Integer, Double> bottomLevels = calculateBottomLevels(dag, tasks, avgCapacity, ccr);
        
        // Calculate top-level (t-level) for each task
        Map<Integer, Double> topLevels = calculateTopLevels(dag, tasks, avgCapacity, ccr);
        
        // Priority = b-level + t-level (critical path length through this task)
        for (int taskId : tasks.keySet()) {
            double priority = bottomLevels.getOrDefault(taskId, 0.0) + 
                            topLevels.getOrDefault(taskId, 0.0);
            priorities.put(taskId, priority);
        }
        
        return priorities;
    }
    
    private static Map<Integer, Double> calculateBottomLevels(
            WorkflowDAG dag,
            Map<Integer, Task> tasks,
            double avgCapacity,
            double ccr
    ) {
        Map<Integer, Double> bottomLevels = new HashMap<>();
        
        // Process in reverse topological order
        List<Integer> reverseTopo = new ArrayList<>(dag.topologicalSort());
        Collections.reverse(reverseTopo);
        
        for (int taskId : reverseTopo) {
            Task task = tasks.get(taskId);
            double executionTime = task.size / avgCapacity;
            
            double maxSuccessorLevel = 0.0;
            for (int successor : dag.getSuccessors(taskId)) {
                double communicationTime = calculateCommunicationTime(task, tasks.get(successor), ccr, avgCapacity);
                double successorLevel = bottomLevels.getOrDefault(successor, 0.0);
                maxSuccessorLevel = Math.max(maxSuccessorLevel, communicationTime + successorLevel);
            }
            
            bottomLevels.put(taskId, executionTime + maxSuccessorLevel);
        }
        
        return bottomLevels;
    }
    
    private static Map<Integer, Double> calculateTopLevels(
            WorkflowDAG dag,
            Map<Integer, Task> tasks,
            double avgCapacity,
            double ccr
    ) {
        Map<Integer, Double> topLevels = new HashMap<>();
        
        // Process in topological order
        List<Integer> topoOrder = dag.topologicalSort();
        
        for (int taskId : topoOrder) {
            double maxPredecessorLevel = 0.0;
            
            for (int predecessor : dag.getPredecessors(taskId)) {
                Task predTask = tasks.get(predecessor);
                double predExecutionTime = predTask.size / avgCapacity;
                double communicationTime = calculateCommunicationTime(predTask, tasks.get(taskId), ccr, avgCapacity);
                double predTopLevel = topLevels.getOrDefault(predecessor, 0.0);
                
                maxPredecessorLevel = Math.max(maxPredecessorLevel, 
                    predTopLevel + predExecutionTime + communicationTime);
            }
            
            topLevels.put(taskId, maxPredecessorLevel);
        }
        
        return topLevels;
    }
    
    private static double calculateCommunicationTime(Task fromTask, Task toTask, double ccr, double avgCapacity) {
        // Simple communication time model
        double dataSize = Math.min(fromTask.size, toTask.size) * 0.1; // 10% of smaller task size
        double avgExecutionTime = (fromTask.size + toTask.size) / (2 * avgCapacity);
        return avgExecutionTime * ccr;
    }
    
    /**
     * Get tasks sorted by their rank (for scheduling priority)
     */
    public static List<Integer> getTasksByRank(
            WorkflowDAG dag,
            Map<Integer, Task> tasks,
            List<VM> vmPool,
            double ccr
    ) {
        Map<Integer, Double> ranks = calculateUpwardRanks(dag, tasks, 
            vmPool.stream().mapToDouble(vm -> vm.capacity).average().orElse(1.0),
            tasks.values().stream().mapToDouble(task -> task.size).average().orElse(1.0) * ccr);
        
        return ranks.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue()))
                .map(Map.Entry::getKey)
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
    }
}
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
        
        // Reset VM states
        for (VM vm : vmPool) {
            vm.waitingTasks.clear();
            vm.availableTime = 0.0;
        }
        
        // Phase 1: Schedule critical path tasks using stable matching
        scheduleCriticalPath(criticalPath, tasks, vmPool, schedule, scheduledTasks);
        
        // Phase 2: Schedule remaining tasks in priority order
        scheduleRemainingTasks(dag, tasks, vmPool, schedule, scheduledTasks);
        
        return schedule;
    }
    
    private static void scheduleCriticalPath(List<Integer> criticalPath, 
                                           Map<Integer, Task> tasks,
                                           List<VM> vmPool, 
                                           Map<Integer, Integer> schedule, 
                                           Set<Integer> scheduledTasks) {
        
        // Use stable matching for critical path tasks
        for (int taskId : criticalPath) {
            if (scheduledTasks.contains(taskId)) continue;
            
            Task task = tasks.get(taskId);
            VM bestVM = findBestVMForTask(task, vmPool, schedule, tasks);
            
            if (bestVM != null) {
                assignTaskToVM(task, bestVM, schedule, scheduledTasks);
            }
        }
    }
    
    private static void scheduleRemainingTasks(WorkflowDAG dag, 
                                             Map<Integer, Task> tasks,
                                             List<VM> vmPool, 
                                             Map<Integer, Integer> schedule, 
                                             Set<Integer> scheduledTasks) {
        
        // Get remaining tasks in topological order
        List<Integer> remainingTasks = dag.topologicalSort().stream()
                .filter(taskId -> !scheduledTasks.contains(taskId))
                .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
        
        // Apply game theory based scheduling
        for (int taskId : remainingTasks) {
            Task task = tasks.get(taskId);
            
            // Check if all dependencies are satisfied
            if (areDependenciesSatisfied(taskId, dag, scheduledTasks)) {
                VM bestVM = findBestVMForTask(task, vmPool, schedule, tasks);
                
                if (bestVM != null) {
                    assignTaskToVM(task, bestVM, schedule, scheduledTasks);
                }
            }
        }
        
        // Handle any remaining unscheduled tasks
        for (int taskId : tasks.keySet()) {
            if (!scheduledTasks.contains(taskId)) {
                Task task = tasks.get(taskId);
                VM bestVM = findBestVMForTask(task, vmPool, schedule, tasks);
                
                if (bestVM != null) {
                    assignTaskToVM(task, bestVM, schedule, scheduledTasks);
                }
            }
        }
    }
    
    private static VM findBestVMForTask(Task task, List<VM> vmPool, 
                                       Map<Integer, Integer> schedule, 
                                       Map<Integer, Task> tasks) {
        
        double bestScore = Double.POSITIVE_INFINITY;
        VM bestVM = null;
        
        for (VM vm : vmPool) {
            double score = calculateTaskVMScore(task, vm, schedule, tasks);
            
            if (score < bestScore) {
                bestScore = score;
                bestVM = vm;
            }
        }
        
        return bestVM;
    }
    
    private static double calculateTaskVMScore(Task task, VM vm, 
                                             Map<Integer, Integer> schedule, 
                                             Map<Integer, Task> tasks) {
        
        // Base execution time
        double executionTime = task.size / vm.capacity;
        
        // Load balancing factor
        double loadFactor = vm.waitingTasks.size() + 1;
        
        // Communication cost factor
        double communicationCost = calculateCommunicationCost(task, vm, schedule, tasks);
        
        // Availability penalty
        double availabilityPenalty = vm.availableTime;
        
        // Combined score (lower is better)
        return executionTime * loadFactor + communicationCost + availabilityPenalty * 0.1;
    }
    
    private static double calculateCommunicationCost(Task task, VM vm, 
                                                   Map<Integer, Integer> schedule, 
                                                   Map<Integer, Task> tasks) {
        
        double totalCommCost = 0.0;
        
        // Check communication with already scheduled tasks
        for (Map.Entry<Integer, Integer> entry : schedule.entrySet()) {
            int scheduledTaskId = entry.getKey();
            int scheduledVMId = entry.getValue();
            
            if (scheduledVMId != vm.id) {
                // Different VM - add communication cost
                Task scheduledTask = tasks.get(scheduledTaskId);
                if (scheduledTask != null) {
                    double dataSize = Math.min(task.size, scheduledTask.size) * 0.1;
                    totalCommCost += dataSize / 10.0; // Simplified communication cost
                }
            }
        }
        
        return totalCommCost;
    }
    
    private static void assignTaskToVM(Task task, VM vm, 
                                     Map<Integer, Integer> schedule, 
                                     Set<Integer> scheduledTasks) {
        
        vm.waitingTasks.add(task);
        task.assignedVM = vm.id;
        
        // Update VM available time
        double executionTime = task.size / vm.capacity;
        vm.availableTime += executionTime;
        
        schedule.put(task.id, vm.id);
        scheduledTasks.add(task.id);
    }
    
    private static boolean areDependenciesSatisfied(int taskId, WorkflowDAG dag, 
                                                   Set<Integer> scheduledTasks) {
        List<Integer> predecessors = dag.getPredecessors(taskId);
        return scheduledTasks.containsAll(predecessors);
    }
    
    /**
     * Enhanced scheduling with game theory optimization
     */
    public static Map<Integer, Integer> scheduleWithGameTheory(
            WorkflowDAG dag,
            Map<Integer, Task> tasks,
            List<VM> vmPool,
            List<Integer> criticalPath
    ) {
        
        Map<Integer, Integer> initialSchedule = schedule(dag, tasks, vmPool, criticalPath);
        
        // Apply game theory optimization
        return optimizeWithGameTheory(initialSchedule, dag, tasks, vmPool);
    }
    
    private static Map<Integer, Integer> optimizeWithGameTheory(
            Map<Integer, Integer> initialSchedule,
            WorkflowDAG dag,
            Map<Integer, Task> tasks,
            List<VM> vmPool
    ) {
        
        Map<Integer, Integer> optimizedSchedule = new HashMap<>(initialSchedule);
        boolean improved = true;
        int maxIterations = 10;
        int iteration = 0;
        
        while (improved && iteration < maxIterations) {
            improved = false;
            iteration++;
            
            // Try to improve each task's assignment
            for (int taskId : tasks.keySet()) {
                int currentVM = optimizedSchedule.get(taskId);
                Task task = tasks.get(taskId);
                
                // Find potential better VMs
                for (VM vm : vmPool) {
                    if (vm.id != currentVM) {
                        // Calculate benefit of moving task to this VM
                        double benefit = calculateMoveBenefit(taskId, vm.id, optimizedSchedule, 
                                                            dag, tasks, vmPool);
                        
                        if (benefit > 0) {
                            // Move task to better VM
                            optimizedSchedule.put(taskId, vm.id);
                            improved = true;
                            break;
                        }
                    }
                }
            }
        }
        
        return optimizedSchedule;
    }
    
    private static double calculateMoveBenefit(int taskId, int newVMId, 
                                             Map<Integer, Integer> schedule,
                                             WorkflowDAG dag, 
                                             Map<Integer, Task> tasks,
                                             List<VM> vmPool) {
        
        // Calculate current cost
        double currentCost = calculateScheduleCost(schedule, dag, tasks, vmPool);
        
        // Create modified schedule
        Map<Integer, Integer> modifiedSchedule = new HashMap<>(schedule);
        modifiedSchedule.put(taskId, newVMId);
        
        // Calculate new cost
        double newCost = calculateScheduleCost(modifiedSchedule, dag, tasks, vmPool);
        
        // Return benefit (positive means improvement)
        return currentCost - newCost;
    }
    
    private static double calculateScheduleCost(Map<Integer, Integer> schedule,
                                              WorkflowDAG dag,
                                              Map<Integer, Task> tasks,
                                              List<VM> vmPool) {
        
        // Simple cost function: weighted sum of execution time and communication cost
        double totalCost = 0.0;
        
        // Execution cost
        for (Map.Entry<Integer, Integer> entry : schedule.entrySet()) {
            int taskId = entry.getKey();
            int vmId = entry.getValue();
            
            Task task = tasks.get(taskId);
            VM vm = vmPool.stream().filter(v -> v.id == vmId).findFirst().orElse(null);
            
            if (vm != null) {
                totalCost += task.size / vm.capacity;
            }
        }
        
        // Communication cost
        for (int taskId : tasks.keySet()) {
            for (int successor : dag.getSuccessors(taskId)) {
                int taskVM = schedule.get(taskId);
                int successorVM = schedule.get(successor);
                
                if (taskVM != successorVM) {
                    Task task = tasks.get(taskId);
                    Task succTask = tasks.get(successor);
                    double commCost = Math.min(task.size, succTask.size) * 0.1;
                    totalCost += commCost;
                }
            }
        }
        
        return totalCost;
    }
}
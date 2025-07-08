package com.example.scheduler;

import com.example.scheduler.model.Task;
import com.example.scheduler.model.VM;
import com.example.scheduler.scheduler.DCP;
import com.example.scheduler.scheduler.SMGT;
import com.example.scheduler.scheduler.LOTD;
import com.example.scheduler.utils.MetricsCalculator;
import com.example.scheduler.utils.Plotter;
import com.example.scheduler.utils.WorkflowDAG;

import java.util.*;

public class Main {

    public static void main(String[] args) {
        // Test different problem sizes
        int[] taskCounts = {20, 50, 100};
        int[] vmCounts = {3, 5, 10};
        double[] ccrValues = {0.1, 0.5, 1.0, 2.0};
        
        System.out.println("=== SCHEDULER ALGORITHM COMPARISON ===\n");
        
        for (int numTasks : taskCounts) {
            for (int numVMs : vmCounts) {
                System.out.printf("Testing with %d tasks and %d VMs:\n", numTasks, numVMs);
                System.out.println("Algorithm\t\tCCR\tSLR\tAVU\tVF\tMakespan");
                System.out.println("--------------------------------------------------------");
                
                for (double ccr : ccrValues) {
                    runComparison(numTasks, numVMs, ccr);
                }
                System.out.println();
            }
        }
        
        // Detailed analysis for one specific case
        System.out.println("=== DETAILED ANALYSIS (50 tasks, 5 VMs) ===");
        runDetailedAnalysis(50, 5);
    }
    
    private static void runComparison(int numTasks, int numVMs, double ccr) {
        Random rand = new Random(42); // Fixed seed for reproducibility
        
        // Generate tasks
        Map<Integer, Task> taskMap = generateTasks(numTasks, rand);
        
        // Generate VMs
        List<VM> vmPool = generateVMs(numVMs, rand);
        
        // Generate DAG
        WorkflowDAG dag = generateDAG(numTasks, rand);
        
        // Test different algorithms
        testRandomScheduling(taskMap, vmPool, dag, ccr);
        testSMGTScheduling(taskMap, vmPool, dag, ccr);
        testLOTDScheduling(taskMap, vmPool, dag, ccr);
    }
    
    private static Map<Integer, Task> generateTasks(int numTasks, Random rand) {
        Map<Integer, Task> taskMap = new HashMap<>();
        for (int i = 1; i <= numTasks; i++) {
            double size = 10 + rand.nextDouble() * 90;  // size between 10 and 100
            taskMap.put(i, new Task(i, size));
        }
        return taskMap;
    }
    
    private static List<VM> generateVMs(int numVMs, Random rand) {
        List<VM> vmPool = new ArrayList<>();
        for (int i = 0; i < numVMs; i++) {
            double capacity = 5 + rand.nextDouble() * 15; // capacity between 5 and 20
            vmPool.add(new VM(i, capacity));
        }
        return vmPool;
    }
    
    private static WorkflowDAG generateDAG(int numTasks, Random rand) {
        WorkflowDAG dag = new WorkflowDAG();
        
        // Create a more structured DAG with layers
        int layers = Math.max(3, numTasks / 10);
        int tasksPerLayer = numTasks / layers;
        
        for (int layer = 0; layer < layers - 1; layer++) {
            int startTask = layer * tasksPerLayer + 1;
            int endTask = Math.min((layer + 1) * tasksPerLayer, numTasks);
            int nextLayerStart = endTask + 1;
            int nextLayerEnd = Math.min(nextLayerStart + tasksPerLayer - 1, numTasks);
            
            for (int from = startTask; from <= endTask; from++) {
                for (int to = nextLayerStart; to <= nextLayerEnd; to++) {
                    if (to <= numTasks && rand.nextDouble() < 0.3) {
                        dag.addEdge(from, to);
                    }
                }
            }
        }
        
        return dag;
    }
    
    private static void testRandomScheduling(Map<Integer, Task> taskMap, List<VM> vmPool, 
                                           WorkflowDAG dag, double ccr) {
        // Reset VM states
        resetVMs(vmPool);
        Map<Integer, Task> taskMapCopy = copyTaskMap(taskMap);
        
        Random rand = new Random(42);
        Map<Integer, Integer> schedule = new HashMap<>();
        
        for (int tid : taskMapCopy.keySet()) {
            int vmId = rand.nextInt(vmPool.size());
            schedule.put(tid, vmId);
        }
        
        MetricsCalculator.Metrics metrics = MetricsCalculator.computeMetrics(
                dag, vmPool, schedule, ccr, taskMapCopy
        );
        
        System.out.printf("Random\t\t\t%.1f\t%.3f\t%.3f\t%.3f\t%.2f\n", 
                         ccr, metrics.slr, metrics.avu, metrics.vf, metrics.makespan);
    }
    
    private static void testSMGTScheduling(Map<Integer, Task> taskMap, List<VM> vmPool, 
                                         WorkflowDAG dag, double ccr) {
        // Reset VM states
        resetVMs(vmPool);
        Map<Integer, Task> taskMapCopy = copyTaskMap(taskMap);
        
        List<Integer> criticalPath = DCP.determineCriticalPath(dag, taskMapCopy, vmPool, ccr);
        Map<Integer, Integer> schedule = SMGT.schedule(dag, taskMapCopy, vmPool, criticalPath);
        
        MetricsCalculator.Metrics metrics = MetricsCalculator.computeMetrics(
                dag, vmPool, schedule, ccr, taskMapCopy
        );
        
        System.out.printf("SMGT\t\t\t%.1f\t%.3f\t%.3f\t%.3f\t%.2f\n", 
                         ccr, metrics.slr, metrics.avu, metrics.vf, metrics.makespan);
    }
    
    private static void testLOTDScheduling(Map<Integer, Task> taskMap, List<VM> vmPool, 
                                         WorkflowDAG dag, double ccr) {
        // Reset VM states
        resetVMs(vmPool);
        Map<Integer, Task> taskMapCopy = copyTaskMap(taskMap);
        
        // First get initial schedule from SMGT
        List<Integer> criticalPath = DCP.determineCriticalPath(dag, taskMapCopy, vmPool, ccr);
        Map<Integer, Integer> initialSchedule = SMGT.schedule(dag, taskMapCopy, vmPool, criticalPath);
        
        // Apply LOTD optimization
        Map<Integer, Integer> optimizedSchedule = applyLOTDOptimization(
                initialSchedule, dag, taskMapCopy, vmPool, ccr
        );
        
        MetricsCalculator.Metrics metrics = MetricsCalculator.computeMetrics(
                dag, vmPool, optimizedSchedule, ccr, taskMapCopy
        );
        
        System.out.printf("SMGT+LOTD\t\t%.1f\t%.3f\t%.3f\t%.3f\t%.2f\n", 
                         ccr, metrics.slr, metrics.avu, metrics.vf, metrics.makespan);
    }
    
    private static Map<Integer, Integer> applyLOTDOptimization(Map<Integer, Integer> schedule, 
                                                             WorkflowDAG dag, 
                                                             Map<Integer, Task> taskMap,
                                                             List<VM> vmPool, double ccr) {
        // Simple LOTD: duplicate critical tasks to reduce communication overhead
        Map<Integer, Integer> optimizedSchedule = new HashMap<>(schedule);
        
        List<Integer> criticalPath = DCP.determineCriticalPath(dag, taskMap, vmPool, ccr);
        
        // For each critical task, check if duplication would help
        for (int taskId : criticalPath) {
            List<Integer> successors = dag.getSuccessors(taskId);
            if (!successors.isEmpty()) {
                // Find the VM with most successors
                Map<Integer, Integer> vmSuccessorCount = new HashMap<>();
                for (int succ : successors) {
                    int vmId = schedule.get(succ);
                    vmSuccessorCount.put(vmId, vmSuccessorCount.getOrDefault(vmId, 0) + 1);
                }
                
                int bestVM = vmSuccessorCount.entrySet().stream()
                        .max(Map.Entry.comparingByValue())
                        .map(Map.Entry::getKey)
                        .orElse(schedule.get(taskId));
                
                // If beneficial, move the task to the VM with most successors
                if (bestVM != schedule.get(taskId) && vmSuccessorCount.get(bestVM) > 1) {
                    optimizedSchedule.put(taskId, bestVM);
                }
            }
        }
        
        return optimizedSchedule;
    }
    
    private static void resetVMs(List<VM> vmPool) {
        for (VM vm : vmPool) {
            vm.availableTime = 0.0;
            vm.waitingTasks.clear();
        }
    }
    
    private static Map<Integer, Task> copyTaskMap(Map<Integer, Task> original) {
        Map<Integer, Task> copy = new HashMap<>();
        for (Map.Entry<Integer, Task> entry : original.entrySet()) {
            Task originalTask = entry.getValue();
            Task copiedTask = new Task(originalTask.id, originalTask.size);
            copy.put(entry.getKey(), copiedTask);
        }
        return copy;
    }
    
    private static void runDetailedAnalysis(int numTasks, int numVMs) {
        Random rand = new Random(42);
        Map<Integer, Task> taskMap = generateTasks(numTasks, rand);
        List<VM> vmPool = generateVMs(numVMs, rand);
        WorkflowDAG dag = generateDAG(numTasks, rand);
        
        List<Double> ccrValues = new ArrayList<>();
        List<Double> randomSLR = new ArrayList<>();
        List<Double> smgtSLR = new ArrayList<>();
        List<Double> lotdSLR = new ArrayList<>();
        
        List<Double> randomAVU = new ArrayList<>();
        List<Double> smgtAVU = new ArrayList<>();
        List<Double> lotdAVU = new ArrayList<>();
        
        double[] testCCRs = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.5, 2.0};
        
        for (double ccr : testCCRs) {
            ccrValues.add(ccr);
            
            // Random scheduling
            resetVMs(vmPool);
            Map<Integer, Task> taskMapCopy1 = copyTaskMap(taskMap);
            Map<Integer, Integer> randomSchedule = new HashMap<>();
            Random schedRand = new Random(42);
            for (int tid : taskMapCopy1.keySet()) {
                randomSchedule.put(tid, schedRand.nextInt(numVMs));
            }
            MetricsCalculator.Metrics randomMetrics = MetricsCalculator.computeMetrics(
                    dag, vmPool, randomSchedule, ccr, taskMapCopy1);
            randomSLR.add(randomMetrics.slr);
            randomAVU.add(randomMetrics.avu);
            
            // SMGT scheduling
            resetVMs(vmPool);
            Map<Integer, Task> taskMapCopy2 = copyTaskMap(taskMap);
            List<Integer> criticalPath = DCP.determineCriticalPath(dag, taskMapCopy2, vmPool, ccr);
            Map<Integer, Integer> smgtSchedule = SMGT.schedule(dag, taskMapCopy2, vmPool, criticalPath);
            MetricsCalculator.Metrics smgtMetrics = MetricsCalculator.computeMetrics(
                    dag, vmPool, smgtSchedule, ccr, taskMapCopy2);
            smgtSLR.add(smgtMetrics.slr);
            smgtAVU.add(smgtMetrics.avu);
            
            // LOTD scheduling
            resetVMs(vmPool);
            Map<Integer, Task> taskMapCopy3 = copyTaskMap(taskMap);
            List<Integer> criticalPath2 = DCP.determineCriticalPath(dag, taskMapCopy3, vmPool, ccr);
            Map<Integer, Integer> initialSchedule = SMGT.schedule(dag, taskMapCopy3, vmPool, criticalPath2);
            Map<Integer, Integer> lotdSchedule = applyLOTDOptimization(
                    initialSchedule, dag, taskMapCopy3, vmPool, ccr);
            MetricsCalculator.Metrics lotdMetrics = MetricsCalculator.computeMetrics(
                    dag, vmPool, lotdSchedule, ccr, taskMapCopy3);
            lotdSLR.add(lotdMetrics.slr);
            lotdAVU.add(lotdMetrics.avu);
        }
        
        // Show detailed comparison chart
        showComparisonChart("SLR Comparison", ccrValues, randomSLR, smgtSLR, lotdSLR);
        showComparisonChart("AVU Comparison", ccrValues, randomAVU, smgtAVU, lotdAVU);
        
        // Print summary statistics
        System.out.printf("\nSummary Statistics:\n");
        System.out.printf("Algorithm\tAvg SLR\tAvg AVU\tBest SLR\tBest AVU\n");
        System.out.printf("Random\t\t%.3f\t%.3f\t%.3f\t\t%.3f\n", 
                         randomSLR.stream().mapToDouble(Double::doubleValue).average().orElse(0),
                         randomAVU.stream().mapToDouble(Double::doubleValue).average().orElse(0),
                         randomSLR.stream().mapToDouble(Double::doubleValue).min().orElse(0),
                         randomAVU.stream().mapToDouble(Double::doubleValue).max().orElse(0));
        System.out.printf("SMGT\t\t%.3f\t%.3f\t%.3f\t\t%.3f\n", 
                         smgtSLR.stream().mapToDouble(Double::doubleValue).average().orElse(0),
                         smgtAVU.stream().mapToDouble(Double::doubleValue).average().orElse(0),
                         smgtSLR.stream().mapToDouble(Double::doubleValue).min().orElse(0),
                         smgtAVU.stream().mapToDouble(Double::doubleValue).max().orElse(0));
        System.out.printf("SMGT+LOTD\t%.3f\t%.3f\t%.3f\t\t%.3f\n", 
                         lotdSLR.stream().mapToDouble(Double::doubleValue).average().orElse(0),
                         lotdAVU.stream().mapToDouble(Double::doubleValue).average().orElse(0),
                         lotdSLR.stream().mapToDouble(Double::doubleValue).min().orElse(0),
                         lotdAVU.stream().mapToDouble(Double::doubleValue).max().orElse(0));
    }
    
    private static void showComparisonChart(String title, List<Double> ccrValues, 
                                          List<Double> randomValues, List<Double> smgtValues, 
                                          List<Double> lotdValues) {
        // This would show a comparison chart - implementation depends on your plotting library
        System.out.printf("\n%s vs CCR:\n", title);
        System.out.printf("CCR\tRandom\tSMGT\tLOTD\n");
        for (int i = 0; i < ccrValues.size(); i++) {
            System.out.printf("%.1f\t%.3f\t%.3f\t%.3f\n", 
                             ccrValues.get(i), randomValues.get(i), 
                             smgtValues.get(i), lotdValues.get(i));
        }
    }
}
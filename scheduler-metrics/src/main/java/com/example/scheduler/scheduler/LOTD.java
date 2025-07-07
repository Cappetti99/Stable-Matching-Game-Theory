package com.example.scheduler.scheduler;    

import com.example.scheduler.model.Task;
import com.example.scheduler.model.VM;
import com.example.scheduler.utils.WorkflowDAG;

import java.util.*;

public class LOTD {

    private static class VM {
        int id;
        Queue<Task> waitingList = new LinkedList<>();

        public VM(int id) {
            this.id = id;
        }
    }

    private final List<VM> vmList;
    private final WorkflowDAG dag;
    private final Map<Integer, Double> AST = new HashMap<>();
    private final Map<Integer, Double> AFT = new HashMap<>();
    private final Set<Integer> replicatedTasks = new HashSet<>();

    public LOTD(List<VM> vmList, WorkflowDAG dag) {
        this.vmList = vmList;
        this.dag = dag;
    }

    public void run() {
        // 1. Call DCP and SMGT to generate pre-schedule (omitted)
        // ...

        for (VM vm : vmList) { // Line 2
            if (vm.waitingList.isEmpty()) continue;

            Task t = vm.waitingList.peek(); // Line 3

            if (AST.getOrDefault(t.id, 0.0) != 0.0) { // Line 4
                double minST = Double.POSITIVE_INFINITY;
                Integer minPredecessor = null;

                for (int p : dag.getPredecessors(t.id)) { // Line 8
                    if (replicatedTasks.contains(p)) { // Line 9
                        double st = calculateStartTime(t.id, p); // Line 10

                        if (st < minST) { // Line 12
                            minST = st;
                            minPredecessor = p;
                        }
                    }
                }

                if (minST < AST.getOrDefault(t.id, Double.POSITIVE_INFINITY)) { // Line 17
                    replicateTaskToVM(minPredecessor, vm); // Line 18
                    updateASTandAFT(); // Line 19
                }
            }
        }
    }

    private double calculateStartTime(int taskId, int predecessorId) {
        // Supponiamo che st sia AFT del predecessore + comunicazione (qui 0)
        return AFT.getOrDefault(predecessorId, 0.0);
    }

    private void replicateTaskToVM(Integer taskId, VM vm) {
        if (taskId == null) return;

        replicatedTasks.add(taskId);
        // Crea una copia del task e assegnalo
        Task replicated = new Task(taskId, 0); // size è simbolica qui
        replicated.assignedVM = vm.id;
        vm.waitingList.add(replicated);
    }

    private void updateASTandAFT() {
        // Aggiorna i tempi di inizio/fine seguendo l’ordine topologico
        for (int taskId : dag.topologicalSort()) {
            List<Integer> preds = dag.getPredecessors(taskId);
            double maxAFT = 0;
            for (int p : preds) {
                maxAFT = Math.max(maxAFT, AFT.getOrDefault(p, 0.0));
            }
            double start = maxAFT;
            double finish = start + 1; // Supponiamo che ogni task duri 1

            AST.put(taskId, start);
            AFT.put(taskId, finish);
        }
    }
}
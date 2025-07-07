package com.example.scheduler;

import com.example.scheduler.model.Task;
import com.example.scheduler.model.VM;
import com.example.scheduler.utils.MetricsCalculator;
import com.example.scheduler.utils.Plotter;
import com.example.scheduler.utils.WorkflowDAG;

import java.util.*;

public class Main {

    public static void main(String[] args) {

        int numTasks = 50;
        int numVMs = 5;
        double ccr = 0.5;

        Random rand = new Random();

        // 1. Genera task
        Map<Integer, Task> taskMap = new HashMap<>();
        for (int i = 1; i <= numTasks; i++) {
            double size = 10 + rand.nextDouble() * 90;  // size tra 10 e 100
            taskMap.put(i, new Task(i, size));
        }

        // 2. Genera VM
        List<VM> vmPool = new ArrayList<>();
        for (int i = 0; i < numVMs; i++) {
            double capacity = 5 + rand.nextDouble() * 15; // capacity tra 5 e 20
            vmPool.add(new VM(i, capacity));
        }

        // 3. Genera DAG aciclico casuale
        WorkflowDAG dag = new WorkflowDAG();
        for (int from = 1; from <= numTasks; from++) {
            for (int to = from + 1; to <= numTasks; to++) {
                if (rand.nextDouble() < 0.05) {  // 5% probabilità di edge
                    dag.addEdge(from, to);
                }
            }
        }

        // 4. Assegna uno schedule random
        Map<Integer, Integer> schedule = new HashMap<>();
        for (int tid : taskMap.keySet()) {
            int vmId = rand.nextInt(numVMs);
            schedule.put(tid, vmId);
        }

        // 5. Calcola le metriche
        MetricsCalculator.Metrics metrics = MetricsCalculator.computeMetrics(
                dag, vmPool, schedule, ccr, taskMap
        );

        // 6. Stampa le metriche
        System.out.printf("SLR: %.4f\n", metrics.slr);
        System.out.printf("AVU: %.4f\n", metrics.avu);
        System.out.printf("VF:  %.4f\n", metrics.vf);

        // 7. Mostra il grafico con i dati reali
        List<Double> ccrValues = List.of(ccr);
        List<Double> slrValues = List.of(metrics.slr);
        List<Double> avuValues = List.of(metrics.avu);
        List<Double> vfValues  = List.of(metrics.vf);

        Plotter.show("Performance Metrics", ccrValues, slrValues, avuValues, vfValues, "CCR");
    }
}


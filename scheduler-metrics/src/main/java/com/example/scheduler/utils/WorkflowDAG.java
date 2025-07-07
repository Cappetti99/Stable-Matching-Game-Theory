package com.example.scheduler.utils;

import java.util.*;

public class WorkflowDAG {
    private final Map<Integer, List<Integer>> adjacency = new HashMap<>();

    public void addEdge(int from, int to) {
        adjacency.putIfAbsent(from, new ArrayList<>());
        adjacency.putIfAbsent(to, new ArrayList<>());  // ensure target exists
        adjacency.get(from).add(to);
    }

    public List<Integer> getSuccessors(int taskId) {
        return adjacency.getOrDefault(taskId, new ArrayList<>());
    }

    public List<Integer> getPredecessors(int taskId) {
        List<Integer> preds = new ArrayList<>();
        for (Map.Entry<Integer, List<Integer>> entry : adjacency.entrySet()) {
            if (entry.getValue().contains(taskId)) {
                preds.add(entry.getKey());
            }
        }
        return preds;
    }

    public List<Integer> topologicalSort() {
        List<Integer> result = new ArrayList<>();
        Map<Integer, Integer> inDegree = new HashMap<>();

        for (int node : adjacency.keySet()) {
            inDegree.putIfAbsent(node, 0);
            for (int neighbor : adjacency.get(node)) {
                inDegree.put(neighbor, inDegree.getOrDefault(neighbor, 0) + 1);
            }
        }

        Queue<Integer> queue = new LinkedList<>();
        for (int node : inDegree.keySet()) {
            if (inDegree.get(node) == 0) queue.add(node);
        }

        while (!queue.isEmpty()) {
            int current = queue.poll();
            result.add(current);
            for (int neighbor : getSuccessors(current)) {
                inDegree.put(neighbor, inDegree.get(neighbor) - 1);
                if (inDegree.get(neighbor) == 0) queue.add(neighbor);
            }
        }

        return result;
    }

    public Set<Integer> getAllTasks() {
        return adjacency.keySet();
    }
}
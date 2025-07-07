package com.example.scheduler.model;

import java.util.ArrayList;
import java.util.List;

public class VM {
    public int id;
    public double capacity;
    public double availableTime;
    public List<Task> waitingTasks;
    public int threshold = 2;

    public VM(int id, double capacity) {
        this.id = id;
        this.capacity = capacity;
        this.availableTime = 0.0;
        this.waitingTasks = new ArrayList<>();
    }

    
}
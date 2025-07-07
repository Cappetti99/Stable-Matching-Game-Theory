package com.example.scheduler.model;

public class Task {
    public int id;
    public double size;
    public Integer assignedVM;

    public Task(int id, double size) {
        this.id = id;
        this.size = size;
        this.assignedVM = null;
    }
}

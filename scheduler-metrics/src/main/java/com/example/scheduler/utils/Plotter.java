package com.example.scheduler.utils;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.DefaultCategoryDataset;

import javax.swing.*;
import java.util.List;

/**
 * Classe per visualizzare grafici di metriche (SLR, AVU, VF) rispetto a una variabile (es. CCR).
 */
public class Plotter extends JFrame {

    public Plotter(String title, List<Double> xAxisValues, List<Double> slrValues,
                   List<Double> avuValues, List<Double> vfValues, String xAxisLabel) {
        super(title);

        DefaultCategoryDataset dataset = new DefaultCategoryDataset();

        for (int i = 0; i < xAxisValues.size(); i++) {
            String label = String.format("%.2f", xAxisValues.get(i));
            dataset.addValue(slrValues.get(i), "SLR", label);
            dataset.addValue(avuValues.get(i), "AVU", label);
            dataset.addValue(vfValues.get(i), "VF", label);
        }

        JFreeChart chart = ChartFactory.createLineChart(
                title,              // Chart title
                xAxisLabel,         // X-axis label
                "Metric Value",     // Y-axis label
                dataset,            // Dataset
                PlotOrientation.VERTICAL,
                true, true, false   // Legend, tooltips, URLs
        );

        ChartPanel chartPanel = new ChartPanel(chart);
        setContentPane(chartPanel);
    }

    public static void show(String title, List<Double> xAxisValues, List<Double> slrValues,
                            List<Double> avuValues, List<Double> vfValues, String xAxisLabel) {
        SwingUtilities.invokeLater(() -> {
            Plotter plotter = new Plotter(title, xAxisValues, slrValues, avuValues, vfValues, xAxisLabel);
            plotter.setSize(800, 600);
            plotter.setLocationRelativeTo(null);
            plotter.setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
            plotter.setVisible(true);
        });
    }
}
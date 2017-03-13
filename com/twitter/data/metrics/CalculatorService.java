/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics;

import com.twitter.data.metrics.Model.AggregateModel;

/**
 *
 * @author ankita
 */
public class CalculatorService {

    /*
     * Calculates difference between the last and the current operation timestamp
     */
    public static long calculateDuration(AggregateModel oldModel, AggregateModel newModel) {
        Long duration = 0L;
        if (oldModel == null || newModel == null) {
            return -1L;
        }
        try {
            Long closeTick = newModel.getTimestamp();
            Long openTick = oldModel.getTimestamp();
            duration = closeTick - openTick;
        } catch (Exception ex) {
            System.out.println("Error calculating duration for the user " + ex.getMessage());
        }
        return duration;
    }
    
    /*
     * Calculates the final average
     */
    public static double calculateAverage(AggregateModel model) {
        double avg = 0;
        try {
            if (model.getTotalEntries() == 0) {
                avg = model.getTotalDuration();
            } else {
                avg = model.getTotalDuration() / model.getTotalEntries();
            }
        } catch (Exception ex) {
            System.out.println("Error calculating average " + ex.getMessage());
        }
        return avg;
    }
}

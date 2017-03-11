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
public class Calculator {
    
    //maintains a list of all file names and runs the aggregation on each file to calculate the output
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
    
}

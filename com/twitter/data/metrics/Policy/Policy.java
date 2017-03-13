/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics.Policy;

import com.twitter.data.metrics.CalculatorService;
import com.twitter.data.metrics.Model.AggregateModel;

/**
 *
 * @author ankita
 */
public class Policy {
        
       /*
        * Policy - Ignore the current entry in order to compensate for the missing entry.
        * Helps with not messing with the current average duration
        */
        public static AggregateModel ignoreFirstWrongEntry(AggregateModel prevLog, AggregateModel currLog) {
            AggregateModel policyUpdated = prevLog;
            return policyUpdated;
        }
        
        /*
        * Policy - Assume that the missing operation happened at the same timestamp and calculate the toatl accordingly
        * Eg. :If 2 consecutive "opens" are encountered then assume that a previous "close" happened at the same time as the new "open"
        */
        public static AggregateModel updateDurationWithCurrent(AggregateModel prevLog, AggregateModel currLog) {
            AggregateModel policyUpdated = new AggregateModel();
            //Assume the previous operation to have closed now and add it to the aggregate
            Long duration = CalculatorService.calculateDuration(prevLog, currLog) + prevLog.getTotalDuration();
            policyUpdated.setTotalDuration(duration);
            policyUpdated.setTimestamp(currLog.getTimestamp());
            policyUpdated.setOperationType(currLog.getOperationType());
            policyUpdated.setTotalEntries(prevLog.getTotalEntries()+1);
            System.out.println("using update duration with current");
            
            return policyUpdated;
        }
}

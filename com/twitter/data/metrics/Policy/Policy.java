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

        public static AggregateModel ignoreFirstWrongEntry(AggregateModel prevLog, AggregateModel currLog) {
            AggregateModel policyUpdated = prevLog;
            return policyUpdated;
        }

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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics.Model;


public class AggregateModel {
    private Long userId;
    private Long totalDuration = 0L;
    private Long totalEntries = 0L; 
    private long timestamp;
    private String operationType;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getOperationType() {
        return operationType;
    }

    public void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    public Long getTotalDuration() {
        return totalDuration;
    }

    public void setTotalDuration(Long totalDuration) {
        this.totalDuration = totalDuration;
    }

    /*@Override
    public String toString() {
        return totalDuration + "," + totalEntries;
    }*/

    @Override
    public String toString() {
        return "AggregateModel{" + "userId=" + userId + ", totalDuration=" + totalDuration + ", totalEntries=" + totalEntries + ", timestamp=" + timestamp + ", operationType=" + operationType + '}';
    }
    

    public Long getTotalEntries() {
        return totalEntries;
    }

    public void setTotalEntries(Long totalEntries) {
        this.totalEntries = totalEntries;
    }
}

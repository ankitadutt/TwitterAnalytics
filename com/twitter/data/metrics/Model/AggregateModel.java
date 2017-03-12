/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics.Model;

import java.util.Objects;


public class AggregateModel {
    private Long userId;
    private Long totalDuration = 0L;
    private Long totalEntries = 0L; 
    private Long timestamp;
    private String operationType;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
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

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.userId);
        hash = 53 * hash + Objects.hashCode(this.timestamp);
        hash = 53 * hash + Objects.hashCode(this.operationType);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final AggregateModel other = (AggregateModel) obj;
        if (!Objects.equals(this.operationType, other.operationType)) {
            return false;
        }
        if (!Objects.equals(this.userId, other.userId)) {
            return false;
        }
        if (!Objects.equals(this.timestamp, other.timestamp)) {
            return false;
        }
        return true;
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

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics.Model;

/**
 *
 * @author ankita
 */
public class AggregateModel {
    private Long userId;
    private Long totalDuration;
    private Long totalEntries;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getTotalDuration() {
        return totalDuration;
    }

    public void setTotalDuration(Long totalDuration) {
        this.totalDuration = totalDuration;
    }

    @Override
    public String toString() {
        return totalDuration + "," + totalEntries;
    }

    public Long getTotalEntries() {
        return totalEntries;
    }

    public void setTotalEntries(Long totalEntries) {
        this.totalEntries = totalEntries;
    }
}

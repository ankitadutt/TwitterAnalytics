/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author ankit
 */
public class TwitterDataMetrics {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        ShardingService ss = new ShardingService();
        List<String> shardFiles = new ArrayList<String>();
        shardFiles = ss.createShards("D:/testTwitter.txt");
        AggregatorService as = new AggregatorService(shardFiles);
        as.aggregateData();
        
        
        //
    }
    
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics;

import com.twitter.data.metrics.Util.Constants;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author ankit
 */
public class TwitterDataMetrics {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        ShardingService ss = new ShardingService();
        List<String> shardFiles = ss.createShards(Constants.INPUT_PATH + Constants.INPUT_FILE);
        if (shardFiles != null) {
            AggregatorService as = new AggregatorService(shardFiles);
            as.aggregateData();
        }
    }

}

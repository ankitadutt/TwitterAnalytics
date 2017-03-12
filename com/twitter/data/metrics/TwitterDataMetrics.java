/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics;

import com.twitter.data.metrics.Util.Constants;
import java.io.IOException;
import java.util.List;


public class TwitterDataMetrics {
    
    private static final Long MAX_USERS, MEMORY;
    private static final int SHARDS;
    private static String policy;
    private static final Long ALLOC_PER_SHARD;

    static {
        try {
            MAX_USERS = Long.parseLong(System.getProperty("MAX_USERS"));
            MEMORY = Long.parseLong(System.getProperty("MEMORY"));
            if(MAX_USERS==null || MEMORY==null){
                SHARDS=Constants.NUM_SHARDS;
                ALLOC_PER_SHARD = Constants.CAPACITY;
            }
            else{
                //While processing a shard in-memory, the system stores approx. 56 bytes of data per user
                //The calculation below approximates the 56 bytes to 80
                SHARDS=(int)(MAX_USERS*10*8)/(int)(MEMORY*1024*1024);
                ALLOC_PER_SHARD = MAX_USERS/SHARDS;
            }
            String policy = System.getProperty("POLICY");
            if (policy == null) {
                policy = "IGNORE_MISSING";
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize system properties");
        }
    }

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException("Please provide input file path");
        }
        String input = args[0].trim();
        
        ShardingService ss = new ShardingService(SHARDS);
        List<String> shardFiles = ss.createShards(input + Constants.INPUT_FILE);
        //Loop until any new shards have been created
        while (shardFiles.size()>0) {
            System.out.print("inside main");
            AggregatorService as = new AggregatorService(shardFiles,policy, ALLOC_PER_SHARD);
            //method returns new shard files for a shard too big for in-memory processing
            shardFiles = as.aggregateData();
            System.out.println("processing new shard..."+shardFiles);
        }
    }

}

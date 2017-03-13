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

    private static Long MAX_USERS = Constants.NUMBER_OF_USERS;
    private static Long MEMORY = Constants.MEMORY_IN_MB;
    private static Long SHARDS;
    private static String POLICY = null;
    private static Long ALLOC_PER_SHARD = Constants.CAPACITY;

    /**
     * @param <error>
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException("Please specify input file path, users, memory and policy");
        }
        try {
            String input = args[0].trim();
            MAX_USERS = Long.parseLong(args[1].trim());
            System.out.println("Max-Users = " + MAX_USERS);
            MEMORY = Long.parseLong(args[2].trim());
            System.out.println("Memory = " + MEMORY+"MB");
            long heapSize = Runtime.getRuntime().totalMemory();
            if(MEMORY*1024*1024 > heapSize){
                            System.out.println("Not enough memory available for processing");
                            return;

            }
            POLICY = args[3].trim();
            System.out.println("Policy = " + POLICY);

            //While processing a shard in-memory, the system stores approx. 56 bytes of data per user
            //The calculation below approximates the 56 bytes to 80
            if (MEMORY != 0) {
                SHARDS = (MAX_USERS * 10 * 8) / (MEMORY * 1024 * 1024);
            }
            if (SHARDS == 0 || SHARDS == null) {
                SHARDS = Constants.NUM_SHARDS;
            }
            System.out.println("Shards = " + SHARDS);
            if (MAX_USERS != 0) {
                ALLOC_PER_SHARD = MAX_USERS / SHARDS;
            }

            if (POLICY == null) {
                POLICY = "IGNORE_MISSING";
            }

            ShardingService ss = new ShardingService(SHARDS);
            List<String> shardFiles = ss.createShards(input);
            //Loop until any new shards have been created
            while (shardFiles.size() > 0) {
                System.out.println(shardFiles.size() + " Shards created");
                AggregatorService as = new AggregatorService(shardFiles, POLICY, ALLOC_PER_SHARD);
                //method returns new shard files for a shard too big for in-memory processing
                shardFiles = as.aggregateData();
                if (shardFiles.size() > 0) {
                    System.out.println("processing new"+ shardFiles.size() +"shard...");
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize with the given parameters. Please try again.");
        }

    }
}

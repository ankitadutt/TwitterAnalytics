/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics;

import com.twitter.data.metrics.Model.AggregateModel;
import com.twitter.data.metrics.Util.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ShardingService {

    final int shards = Constants.NUM_SHARDS;
    List<BufferedWriter> writers;
    List<String> shardFiles;
    String shardFile;
    public ShardingService() {
        //writers = new ArrayList<PrintWriter>();
        writers = new ArrayList<>();
        shardFiles = new ArrayList<>();
    }
    
    public ShardingService(String filename){
        this.shardFile = filename;
    }

    public List<String> createShards(String inputFile) throws IOException {
        BufferedReader reader;
        BufferedReaderIterator readerIter;
        try {
            reader = FileUtil.openFile(inputFile);
            readerIter = new BufferedReaderIterator(reader);
            createShardFiles(Constants.OUTPUT_PATH);
            for (String line : readerIter) {
                String[] inputLine = line.split(Constants.SEPARATOR);
                Long userId = Long.parseLong(inputLine[0]);
                addToShard(userId, line);
            }
        } catch (Exception ex) {
            System.out.println("Error creating shards " + ex.getMessage());
        } finally {
            for (BufferedWriter w : writers) {
                w.close();
            }

        }
       return shardFiles;
    }
    
    //The method handles cases where a shard is still too big for memory (because of uneven distribution of ids)
    public BufferedWriter createNewShard(AggregateModel log) throws IOException{
        //first time when this is created, create a writer and add it to the map of this shard
        FileWriter fw = new FileWriter(shardFile);
        BufferedWriter writer = new BufferedWriter(fw);
        writer = addToShard(writer,log);
        return writer;
    }
    
    
    public BufferedWriter addToShard(BufferedWriter writer, AggregateModel log) throws IOException{
       String line = log.getUserId()+","+log.getTimestamp()+","+log.getOperationType();
       writer.write(line);
    return writer;
    }

    private void createShardFiles(String filePath) {
            String fileName;
            BufferedWriter writer = null;
            FileWriter fw;
        try {
            for (int i = 0; i < shards; i++) {
                fileName = filePath+"shard"+Integer.toString(i);
                fw = new FileWriter(fileName);
                shardFiles.add(fileName);
                writer = new BufferedWriter(fw);
                writers.add(writer);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
    

    private void addToShard(Long userId, String data) {
        try {
            writers.get((int) (userId % shards)).write(data);
            writers.get((int) (userId % shards)).newLine();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
    
    //handle overflowing shards
}

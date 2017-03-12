/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics;

import com.twitter.data.metrics.Util.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class ShardingService {

    final int shards;
    List<BufferedWriter> writers;
    List<String> shardFiles;
    String shardFile;
    public ShardingService(int shards) {
        writers = new ArrayList<>();
        shardFiles = new ArrayList<>();
        this.shards = shards;
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
    
}

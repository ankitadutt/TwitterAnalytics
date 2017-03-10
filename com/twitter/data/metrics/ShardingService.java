/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics;

import com.twitter.data.metrics.Util.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author ankit
 */
public class ShardingService {

    BufferedReader reader;
    final int shards = Constants.NUM_SHARDS;
    //List<PrintWriter> writers;
    List<BufferedWriter> writers;
    List<String> shardFiles;

    BufferedReaderIterator readerIter;

    public ShardingService() {
        //writers = new ArrayList<PrintWriter>();
        writers = new ArrayList<>();
        shardFiles = new ArrayList<>();
    }

    public List<String> createShards(String inputFile) throws IOException {
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
                File file = new File(fileName);
                fw = new FileWriter(fileName);
                shardFiles.add(fileName);
                //String file = FileUtil.createShard(fileName);
                //System.out.println(file);
                
                //writer = FileUtil.getFileWriter("D:/output.txt");
                writer = new BufferedWriter(fw);
                //PrintWriter writer = new PrintWriter(fileName);
                writers.add(writer);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private void addToShard(Long userId, String data) {
        try {
            //writers.get((int) (userId % shards))..println(data);
            writers.get((int) (userId % shards)).write(data);
            writers.get((int) (userId % shards)).newLine();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }
    
    //handle overflowing shards
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics;

import com.twitter.data.metrics.Model.AggregateModel;
import com.twitter.data.metrics.Util.BufferedReaderIterator;
import com.twitter.data.metrics.Util.Constants;
import com.twitter.data.metrics.Util.FileUtil;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AggregatorService {

    private List<String> shardFiles;
    private Map<Long, AggregateModel> aggregateMap;
    private Set<String> unprocessed;
    private Set<String> processed;
    private String fileName;
    //capacity is not expected to exceed above 2,147,483,648
    private static final int capacity = 2;
    private BufferedWriter sm = null;

    private AggregatorService() {
    }

    public AggregatorService(List<String> shardFiles) {
        this.shardFiles = shardFiles;
        this.unprocessed = new HashSet<>(shardFiles);
        processed = new HashSet<>();
        aggregateMap = new HashMap();
    }

    public void aggregateData() {
        try {
            for (String file : unprocessed) {
                if (processed.contains(file)) {
                    continue;
                }
                this.fileName = file;
                aggregateDataForFile(file);
            }
        } catch (Exception ex) {
            System.out.println("Unable to aggregate data: " + ex.getMessage());
        }
    }

    private void aggregateDataForFile(String inputFile) {
        AggregateModel aggregateModel;
        BufferedReaderIterator iter;
        BufferedReader reader = null;
        try {
            aggregateModel = new AggregateModel();
            reader = FileUtil.openFile(inputFile);
            iter = new BufferedReaderIterator(reader);
            for (String line : iter) {
                aggregateModel = getDataObject(line);
                if (aggregateMap.size() == capacity && !aggregateMap.containsKey(aggregateModel.getUserId())) {
                    newShardManager(inputFile, aggregateModel);
                } else {
                if (aggregateModel.getOperationType().equals("open")) {
                    userTickManager(aggregateModel);
                }
                if (aggregateModel.getOperationType().equals("close")) {
                    userTickManager(aggregateModel);

                    }
                }
            }
            FileUtil.createOutputFile(null, aggregateMap);//writeToOutputFile();

        } catch (Exception ex) {
            System.out.println("Unable to process data for file - " + fileName);
        } finally {
            FileUtil.closeFile(reader);
            processed.add(inputFile);
            aggregateMap.clear();
        }
    }

    private void aggregateMapManager(long duration, AggregateModel log) {
        if (log == null) {
            return;
        }
        AggregateModel aggregateModel;
        try {
            aggregateModel = log;
            if (aggregateMap.containsKey(aggregateModel.getUserId())) {
                aggregateModel = aggregateMap.get(aggregateModel.getUserId());
                aggregateModel.setTotalDuration(duration + aggregateModel.getTotalDuration());
                aggregateModel.setTotalEntries(aggregateModel.getTotalEntries() + 1);
            } else {
                aggregateModel.setUserId(log.getUserId());
                aggregateModel.setTotalEntries(1L);
                aggregateModel.setTotalDuration(duration);
            }
            aggregateMap.put(log.getUserId(), aggregateModel);
        } catch (Exception ex) {
            System.out.println("Aggregate Map Manager");
        }
    }

    public void newShardManager(String inputFile, AggregateModel log) throws IOException {
        String newFile = inputFile + "_1";
        //ShardingService ss = new ShardingService(newFile);//use a common object throughout the class
        /*if (!unprocessed.contains(newFile)) {
            sm = ss.createNewShard(log);
            unprocessed.add(newFile);
        } else {
            sm = ss.addToShard(sm, log);
        }
        FileUtil.closeFile(sm);*/
        String line = log.getUserId()+","+log.getTimestamp()+","+log.getOperationType();
        FileUtil.createFileShard(newFile, line);
        unprocessed.add(newFile);
    }

    //changes to policy can be updated here
    private void userTickManager(AggregateModel log) {
        if (log == null) {
            return;
        }
        AggregateModel newModel = new AggregateModel();
        try {
            if (aggregateMap.containsKey(log.getUserId())) {
                AggregateModel temp = aggregateMap.get(log.getUserId());
                if (log.getOperationType().equals("close") && temp.getOperationType().equals("open")) {
                    Long newDuration = Calculator.calculateDuration(temp, log) + temp.getTotalDuration();
                    if (newDuration == -1) {
                        newDuration = temp.getTotalDuration(); //ignore the errored entry
                    }
                    Long entries = temp.getTotalEntries() + 1;
                    newModel.setUserId(log.getUserId());
                    newModel.setTotalDuration(newDuration);
                    newModel.setTotalEntries(entries);
                } else {
                    newModel.setTotalDuration(temp.getTotalDuration());
                    newModel.setTotalEntries(temp.getTotalEntries());
                    newModel.setTimestamp(log.getTimestamp());
                    newModel.setOperationType(log.getOperationType());
                }
                newModel.setUserId(log.getUserId());
                aggregateMap.put(newModel.getUserId(), newModel);
            } else {
                aggregateMap.put(log.getUserId(), log);
            }
        } catch (Exception ex) {

            System.out.println("Error updating data in map " + ex.getMessage().toString());
        }

    }


    //check where should this file be placed logically
    private AggregateModel getDataObject(String currentLine) {
        AggregateModel aggregateModel = new AggregateModel();
        try {
            String[] currLine = currentLine.split(",");
            Long userId = Long.parseLong(currLine[0]);
            aggregateModel.setUserId(userId);
            Long timestamp = Long.parseLong(currLine[1]);
            aggregateModel.setTimestamp(timestamp);
            aggregateModel.setOperationType(currLine[2]);
        } catch (Exception ex) {
            System.out.println("Unable to read from file-" + ex.getMessage());
        }
        return aggregateModel;
    }

    //put this in a separate class
    /*private void writeToOutputFile() throws IOException {
        fileName = Constants.OUTPUT_PATH + Constants.OUTPUT_FILE;
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            fw = new FileWriter(fileName);
            bw = new BufferedWriter(fw);
            for (AggregateModel m : aggregateMap.values()) {
                double avg = m.getTotalDuration() / m.getTotalEntries();
                bw.write(String.valueOf(m.getUserId()) + "," + String.valueOf(avg));
                bw.newLine();
            }

        } catch (Exception ex) {
            System.out.println("Error writing to final output file "+ex.getMessage().toString());
        } finally {
            FileUtil.closeFile(bw);
        }
    }*/
}

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
                        //do something?
                    }
                }
            }
            writeToOutputFile();

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
        BufferedWriter writer = null;
        ShardingService ss = new ShardingService(newFile);
        if (!unprocessed.contains(newFile)) {
            writer = ss.createNewShard(log);
            unprocessed.add(newFile);
        } else {
            writer = ss.addToShard(writer, log);
        }
        FileUtil.closeFile(writer);
    }

    //changes to policy can be updated here
    private void userTickManager(AggregateModel log) {
        if (log == null) {
            return;
        }
        AggregateModel newModel;
        try {
            if (aggregateMap.containsKey(log.getUserId())) {
                AggregateModel temp = aggregateMap.get(log.getUserId());
                Long newDuration = calculateDuration(temp, log);
                Long entries = log.getTotalEntries() + 1;
                if (newDuration != -1) {
                    newModel = new AggregateModel();
                    newModel.setUserId(log.getUserId());
                    newModel.setTotalDuration(newDuration);
                    newModel.setTotalEntries(entries);
                } else {
                    newModel = log;
                }
                aggregateMap.put(newModel.getUserId(), newModel);
            }
            aggregateMap.put(log.getUserId(), log);
        } catch (Exception ex) {

            System.out.println("Error updating data in map " + ex.getMessage().toString());
        }

    }

    //maintains a list of all file names and runs the aggregation on each file to calculate the output
    private long calculateDuration(AggregateModel oldModel, AggregateModel newModel) {
        Long duration = 0L;
        if (oldModel == null || newModel == null) {
            return -1L;
        }
        try {
            if (oldModel.getOperationType().equals("open") && newModel.getOperationType().equals("close")) {
                Long closeTick = newModel.getTimestamp();
                Long openTick = oldModel.getTimestamp();
                duration = closeTick - openTick;
            }
        } catch (Exception ex) {
            System.out.println("Error calculating duration for the user " + ex.getMessage());
        }
        return duration;
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
    private void writeToOutputFile() throws IOException {
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
    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics;

import com.twitter.data.metrics.Model.AggregateModel;
import com.twitter.data.metrics.Policy.Policy;
import com.twitter.data.metrics.Util.BufferedReaderIterator;
import com.twitter.data.metrics.Util.FileUtil;
import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AggregatorService {

    private Map<Long, AggregateModel> aggregateMap;
    private Set<String> unprocessed;
    private Set<String> processed;
    private String fileName;
    private final String policy;
    private final long capacity;


    /**
     *
     * @param shardFiles
     */
    public AggregatorService(List<String> shardFiles, String policy, Long capacity) {
        this.unprocessed = new HashSet<>(shardFiles);
        this.policy = policy;
        processed = new HashSet<>();
        aggregateMap = new HashMap();
        this.capacity = capacity;
    }

    public List<String> aggregateData() {
        List<String> newShards = new ArrayList<>();
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
        unprocessed.removeAll(processed);
        newShards.addAll(unprocessed);
        return newShards;
    }

    private void aggregateDataForFile(String inputFile) {
        AggregateModel aggregateModel;
        BufferedReaderIterator iter;
        BufferedReader reader = null;
        try {
            reader = FileUtil.openFile(inputFile);
            iter = new BufferedReaderIterator(reader);
            for (String line : iter) {
                aggregateModel = getDataObject(line);
                //when the in-memory capacity should not be overloaded
                if (aggregateMap.size() == capacity && !aggregateMap.containsKey(aggregateModel.getUserId())) {
                    newShardManager(inputFile, aggregateModel);//request another shard
                } else {
                    userTickManager(aggregateModel);
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

    private void newShardManager(String inputFile, AggregateModel log) throws IOException {
        String newFile = inputFile + "_1";
        String line = log.getUserId() + "," + log.getTimestamp() + "," + log.getOperationType();
        FileUtil.createFile(newFile, line);
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
                    Long newDuration = CalculatorService.calculateDuration(temp, log) + temp.getTotalDuration();
                    if (newDuration == -1) {
                        newDuration = temp.getTotalDuration(); //ignore the errored entry
                    }
                    Long entries = temp.getTotalEntries() + 1;
                    newModel.setTotalDuration(newDuration);
                    newModel.setTotalEntries(entries);
                    newModel.setTimestamp(log.getTimestamp());
                    newModel.setOperationType(log.getOperationType());
                } else if(log.getOperationType().equals(temp.getOperationType())){
                    if(policy.toUpperCase().equals("UPDATE_WITH_CURRENT"))
                    newModel = Policy.updateDurationWithCurrent(temp, log);
                    else
                    newModel = Policy.ignoreFirstWrongEntry(temp, log);
                }
                else{
                    
                    newModel.setTotalDuration(temp.getTotalDuration());
                    newModel.setTotalEntries(temp.getTotalEntries());
                    newModel.setTimestamp(log.getTimestamp());
                    newModel.setOperationType(log.getOperationType());
                
                }
                newModel.setUserId(log.getUserId());
                System.out.println(newModel.toString());
                aggregateMap.put(newModel.getUserId(), newModel);
            } else {
                //First log entry for userid
                aggregateMap.put(log.getUserId(), log);
            }
        } catch (Exception ex) {

            System.out.println("Error updating data in map " + ex.getMessage());
        }

    }

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

}

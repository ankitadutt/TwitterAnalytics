/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.twitter.data.metrics;

import com.twitter.data.metrics.Model.AggregateModel;
import com.twitter.data.metrics.Model.LogModel;
import com.twitter.data.metrics.Util.BufferedReaderIterator;
import com.twitter.data.metrics.Util.Constants;
import com.twitter.data.metrics.Util.FileUtil;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author ankit
 */
public class AggregatorService {

    private List<String> shardFiles;
    private Map<Long, Long> userTick; //stores the userId and duration
    private Map<Long, AggregateModel> aggregateMap;
    private LogModel logModel;
    private BufferedReader reader;
    private BufferedReaderIterator iter;
    private AggregateModel aggregateModel;
    private BufferedWriter writer;
    
    private AggregatorService() {
    }

    public AggregatorService(List<String> shardFiles) {
        this.shardFiles = shardFiles;
        userTick = new HashMap<>();
        aggregateMap = new HashMap();
    }

    public void aggregateData() {
        try {
            for (String file : shardFiles) {
                aggregateDataForFile(file);
            }
        } catch (Exception ex) {
           System.out.println("Unable to aggregate data");
        }
    }

    private void aggregateDataForFile(String inputFile) {
        try {
            reader = FileUtil.openFile(inputFile);
            iter = new BufferedReaderIterator(reader);
            long duration;
            for (String line : iter) {
                logModel = getLogObject(line);
                if (logModel.getOperationType().equals("open")) {
                    userTickManager(logModel);
                }
                if (logModel.getOperationType().equals("close")) {
                    //calculate duration
                    duration = calculateDuration(logModel);
                    //update aggregateMap
                    aggregateMapManager(duration, logModel);
                    //remove from userTick
                    userTick.remove(logModel.getUserID());
                }
            }
            writeToOutputFile();

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            FileUtil.closeFile(reader);
        }
        //read through the shardFile
        //for each user, populate the map of userId and open time
        //when close is encounterd, add it to a new map
    }

    private void aggregateMapManager(long duration, LogModel log) {
        if (log == null) {
            return;
        }
        aggregateModel = new AggregateModel();
        try {
            logModel = log;
            if (aggregateMap.containsKey(logModel.getUserID())) {
                aggregateModel = aggregateMap.get(logModel.getUserID());
                aggregateModel.setTotalDuration(duration + aggregateModel.getTotalDuration());
                aggregateModel.setTotalEntries(aggregateModel.getTotalEntries() + 1);
            } else {
                aggregateModel.setUserId(log.getUserID());
                aggregateModel.setTotalEntries(1L);
                aggregateModel.setTotalDuration(duration);
            }
            aggregateMap.put(log.getUserID(), aggregateModel);
        } catch (Exception ex) {

            ex.printStackTrace();

        }
    }
    //changes to policy can be updated here

    private void userTickManager(LogModel log) {
        if (log == null) {
            return;
        }
        logModel = log;
        try {
            if (userTick.containsKey(logModel.getUserID())) {
                //implement policy where 2 open values are encounterd successively
            }
            userTick.put(logModel.getUserID(), logModel.getTimestamp());
        } catch (Exception ex) {

            ex.printStackTrace();
        }

    }

    //maintains a list of all file names and runs the aggregation on each file to calculate the output
    private long calculateDuration(LogModel logObject) {
        Long duration = 0L;
        try {
            aggregateModel = new AggregateModel();
            Long closeTick = logObject.getTimestamp();
            Long openTick = userTick.get(logObject.getUserID());
            duration = closeTick - openTick;
        } catch (Exception ex) {
            System.out.println("Error calculating duration for the user " + ex.getMessage());
        }

        return duration;
    }

    //check where should this file be placed logically
    private LogModel getLogObject(String currentLine) {
        logModel = new LogModel();
        try {
            String[] currLine = currentLine.split(",");
            Long userId = Long.parseLong(currLine[0]);
            logModel.setUserID(userId);
            Long timestamp = Long.parseLong(currLine[1]);
            logModel.setTimestamp(timestamp);
            logModel.setOperationType(currLine[2]);
        } catch (Exception ex) {
            System.out.println("Unable to read from file-" + ex.getMessage());
        }
        return logModel;
    }

    //put this in a separate class
    private void writeToOutputFile() throws IOException {
        String fileName;
        fileName = Constants.OUTPUT_PATH + Constants.OUTPUT_FILE;
        FileWriter fw = null;
        try {
            fw = new FileWriter(fileName);
            writer = new BufferedWriter(fw);
            for(AggregateModel m:aggregateMap.values()){
            //refactor this part
            double avg = m.getTotalDuration()/m.getTotalEntries();
            writer.write(String.valueOf(m.getUserId())+","+String.valueOf(avg));
            writer.newLine();
            
            }
            
        } catch (Exception ex) {
            System.out.println("Error writing to final output file");
        }
        finally{
            writer.close();
        }
    }

}
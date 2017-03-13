package com.twitter.data.metrics.Util;

import com.twitter.data.metrics.CalculatorService;
import com.twitter.data.metrics.Model.AggregateModel;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Map;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
public class FileUtil {

    public static BufferedReader openFile(String filePath) throws FileNotFoundException {
        if (filePath != null) {
            return new BufferedReader(new InputStreamReader(new FileInputStream(filePath)));
        }
        return null;
    }

    public static void closeFile(BufferedReader br) {
        if (br != null) {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void closeFile(BufferedWriter br) {
        if (br != null) {
            try {
                br.flush();
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void deleteFile(String file) {
        if (file != null) {
            File delFile = new File(file);
            delFile.delete();
        }
    }

    public static void createOutputFile(String file, Map<Long, AggregateModel> map) {
        if (file == null) {
            file = Constants.OUTPUT_PATH + Constants.OUTPUT_FILE;
        }
        else{
            File f = new File(file);
            file = f.getParent()+"/"+Constants.OUTPUT_FILE;
        }
        try (FileWriter fw = new FileWriter(file, true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw)) {
            for (AggregateModel model : map.values()) {
                out.println(model.getUserId() + "," + model.getTotalDuration() + "," + model.getTotalEntries()+","+CalculatorService.calculateAverage(model));

            }
        } catch (IOException e) {
            System.out.println("Error writing to final output file " + e.getMessage());
        }

    }

    public static void createFile(String file, String line) {
        if (file == null) {
            return;
        }
        try (FileWriter fw = new FileWriter(file, true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw)) {
            out.println(line);
        } catch (IOException e) {
            System.out.println("Error writing to final output file " + e.getMessage());
        }

    }
}

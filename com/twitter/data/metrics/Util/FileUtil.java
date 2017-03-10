package com.twitter.data.metrics.Util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author ankita
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
    
    public static String createShard(String file) throws IOException {
        if (file == null) {
            return null;
        }
        File tempFile = File.createTempFile(file, ".tmp", new File(file));
        tempFile.deleteOnExit();
        return tempFile.getAbsolutePath();
    }
}

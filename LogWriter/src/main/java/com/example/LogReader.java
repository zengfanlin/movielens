package com.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class LogReader {
    static final Log logger = LogFactory.getLog(LogReader.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        SnowFlake snowFlake = new SnowFlake(2, 3);
        Thread.sleep(1000);
        FileReader fr = new FileReader("D:\\00-workspace\\dataset\\ml-100k\\u.data"); // 绝对路径或相对路径都可以，写入文件时演示相对路径,读取以上路径的input.txt文件
        BufferedReader br = new BufferedReader(fr);
        String line = "";
        int i = 0;
        while ((line = br.readLine()) != null && i < 20) {
            long id = snowFlake.nextId();
//            System.out.println(id + "\t" + line);
            logger.info(id + "\t" + line);
//            logger.info(i);
            i++;
        }
        br.close();
        fr.close();
    }
}

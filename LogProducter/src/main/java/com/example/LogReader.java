package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SpringBootApplication
public class LogReader {
    private static Logger logger = LogManager.getLogger(LogReader.class);
    public static void main(String[] args) {
        SpringApplication.run(LogReader.class, args);
        String pathname = "D:\\00-workspace\\dataset\\ml-100k\\u.data"; // 绝对路径或相对路径都可以，写入文件时演示相对路径,读取以上路径的input.txt文件
        //防止文件建立或读取失败，用catch捕捉错误并打印，也可以throw;
        //不关闭文件会导致资源的泄露，读写文件都同理
        //Java7的try-with-resources可以优雅关闭文件，异常时自动关闭文件；详细解读https://stackoverflow.com/a/12665271
        try (FileReader reader = new FileReader(pathname);
             BufferedReader br = new BufferedReader(reader) // 建立一个对象，它把文件内容转成计算机能读懂的语言
        ) {
            String line;
            int i = 0;
            //网友推荐更加简洁的写法
            while ((line = br.readLine()) != null && i < 10) {
                // 一次读入一行数据
//                System.out.println(line);
                String kvs[] = line.split("\t");
                StringBuilder sb = new StringBuilder();
                for (String kv : kvs) {
                    sb.append(kv + "|");
                }
                logger.info(sb.toString());
                Thread.sleep(200);
                i++;
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}

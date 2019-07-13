package com.example;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Set;

/**
 *
 */
public class jedisTest implements Runnable {
    @Override
    public void run() {
        // 第一步：创建一个JedisPool对象。需要指定服务端的ip及端口。
        JedisPool jedisPool = new JedisPool("nodemysql", 6379);
        // 第二步：从JedisPool中获得Jedis对象。
        Jedis jedis = jedisPool.getResource();
        int num = Integer.parseInt(jedis.get("key2"));// 1: 代码行1

        num = num + 1; // 2: 代码行2
        jedis.set("num", num + "");
        // 第四步：操作完毕后关闭jedis对象，连接池回收资源。
        jedis.close();
        // 第五步：关闭JedisPool对象。
        jedisPool.close();
    }

    public static void main(String[] args) {

        // 第一步：创建一个JedisPool对象。需要指定服务端的ip及端口。
//        JedisPool jedisPool = new JedisPool("nodemysql", 6379);
//        // 第二步：从JedisPool中获得Jedis对象。
//
//
//        Jedis jedis = jedisPool.getResource();
//        Set<String> keys = jedis.keys("*"); //列出所有的key
//        // 第四步：操作完毕后关闭jedis对象，连接池回收资源。
//        jedis.close();
//        // 第五步：关闭JedisPool对象。
//        jedisPool.close();

    }
}

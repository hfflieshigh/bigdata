package com.atguigu.consumer;

import kafka.javaapi.consumer.SimpleConsumer;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xuzl
 * @create 2019-07-19 6:45
 */
public class MySimpleConsumer {

    //消费指定主题、指定分区、指定偏移量数据
    public static void main(String[] args) {
        ArrayList<String> brokers = new ArrayList<String>();
        brokers.add("192.168.25.102");
        brokers.add("192.168.25.103");
        brokers.add("192.168.25.104");
        int port = 9092;
        String topic ="second";
        int partition = 0;
        long offset = 0;


    }

    //获取分区leader
    public void getLeader(List<String> brokers, int port, String tipic, int partition){
        for(String broker : brokers){
            SimpleConsumer consumer = new SimpleConsumer(broker, port, 1000, 1024 * 4
              ,"client");
            consumer.
        }

    }
}

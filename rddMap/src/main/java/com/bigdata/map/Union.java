package com.bigdata.map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Union {
    public static void main(String[] args) {
        // ******************************UNİON: İKİ FARKLI RDD Yİ BİRLEŞTİRMEMİZİ SAĞLIYOR*******************************/
        JavaSparkContext sc = new JavaSparkContext("local", "map func");
        JavaRDD<String> RawData = sc.textFile("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\rddMap\\WorldCups.csv");
        JavaRDD<String> seconRdd = sc
                .textFile("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\rddMap\\WorldCupPlayers.csv");
        JavaRDD<String> resultRdd = RawData.union(seconRdd);

        System.out.println("1.rdd" + RawData.count());
        System.out.println("2.Rdd" + seconRdd.count());
        System.out.println("3.Rdd" + resultRdd.count());
    }
}


package com.bigdata.map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Distinct {
    public static void main(String[] args) {
        
        JavaSparkContext sc = new JavaSparkContext("local", "map func");
        JavaRDD<String> RawData = sc.textFile("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\rddmap\\WorldCups.csv");

        JavaRDD<String> distincRdd = RawData.distinct() ;
       
      System.out.println(distincRdd.count());
}
}

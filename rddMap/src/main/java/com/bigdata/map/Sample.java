package com.bigdata.map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

public class Sample {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local", "map func");
        JavaRDD<String> RawData = sc.textFile("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\rddmap\\WorldCups.csv");
         
        JavaRDD<String> sampleRdd = RawData.sample(false, 0.5);
        sampleRdd.foreach(new VoidFunction<String>() {

            @Override
            public void call(String t) throws Exception {
                System.out.println(t);

            }

        });

    }

}

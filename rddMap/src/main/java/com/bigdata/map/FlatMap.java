package com.bigdata.map;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

public class FlatMap {
    public static void main(String[] args) {
        
        // map ile flatmap in farkı mapde satır satır rdd oluşturuyor flatmap de ise kelime kelime rdd oluşturuyor
        JavaSparkContext sc = new JavaSparkContext("local", "map func");
        JavaRDD<String> RawData = sc.textFile("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\rddmap\\WorldCups.csv");

        JavaRDD<String> flatMapRdd = RawData.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String t) throws Exception {
              return Arrays.asList(t.split(",")).iterator();
            }
            
        }); 
        flatMapRdd.foreach(new VoidFunction<String>() {

            @Override
            public void call(String t) throws Exception {
               System.out.println(t);
                
            }
            
        });    
}

    }
    


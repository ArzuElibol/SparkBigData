package com.bigdata.action;


import java.util.List;


import org.apache.hadoop.mapreduce.OutputFormat;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import com.bigdata.map.CupModel;

public class App {
    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext("local", "map func");
        JavaRDD<String> RawData = sc.textFile("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\rddmap\\WorldCups.csv");

        // **************TAKE-TAKESAMPLE ACTİON METHODLARI***************/
        List<String> take = RawData.take(4);
        for (String x : take) {
            System.out.println(x);
        }

        List<String> tList = RawData.takeSample(false, 3);
        for (String x : tList) {
            System.out.println(x);
        }
      
     
    
//*************************************************************************************** */
               JavaRDD<CupModel> newRdd = RawData.map(new Function<String, CupModel>() {

            @Override

            public CupModel call(String line) throws Exception {
                String[] split = line.split(",");

                return new CupModel(split[0], split[1], split[2], split[3], split[4], split[5],
                        Integer.parseInt(split[6]), Integer.parseInt(split[7]), Integer.parseInt(split[8]),
                        split[9]);
            }
        });

        /// verileri obje değil string olarak kayıt etmek için

        JavaRDD<String> result = newRdd.map(new Function<CupModel, String>() {

            @Override
            public String call(CupModel v1) throws Exception {
                return v1.getYil() + " " + v1.getToplamGol();
            }
        });

        result.saveAsTextFile("F://toplamGol//");
        // ???????????????????????????????????????????????????????????????????????

    }


    

}
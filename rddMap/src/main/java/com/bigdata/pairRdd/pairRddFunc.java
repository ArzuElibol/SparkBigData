package com.bigdata.pairRdd;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import com.bigdata.map.CupModel;

import scala.Tuple2;

public class pairRddFunc {

    public static void main(String[] args) {

        //maptoPair tupple2 yani key value gibi iki alana göre işlem yapabiliyor

        JavaSparkContext sc = new JavaSparkContext("local", "map func");
        JavaRDD<String> RawData = sc.textFile("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\rddmap\\WorldCups.csv");
        System.out.println("veri sayısı=" + RawData.count());

        JavaRDD<CupModel> newRdd = RawData.map(new Function<String, CupModel>() {

            @Override
            public CupModel call(String line) throws Exception {
                String[] split = line.split(",");

                return new CupModel(split[0], split[1], split[2], split[3], split[4], split[5],
                        Integer.parseInt(split[6]), Integer.parseInt(split[7]), Integer.parseInt(split[8]),
                        split[9]);
            }
        });

        JavaPairRDD<String, String> firstPair = newRdd.mapToPair(new PairFunction<CupModel, String, String>() {

            @Override
            public Tuple2<String, String> call(CupModel t) throws Exception {
                return new Tuple2<String, String>(t.getBirinci(), t.getToplamKatilimci());
            }

        });

        firstPair.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> line) throws Exception {
                System.out.println(line._1() + "--" + line._2());
            }
        });


        JavaPairRDD<String, Iterable<String>> resultData = firstPair.groupByKey();
resultData.foreach(new VoidFunction<Tuple2<String,Iterable<String>>>() {

    @Override
    public void call(Tuple2<String, Iterable<String>> t) throws Exception {
     System.out.println(t._1()+"--"+t._2());
    }
    
});



    }

}
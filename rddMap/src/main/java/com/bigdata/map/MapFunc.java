package com.bigdata.map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import com.bigdata.map.CupModel;

import scala.reflect.api.Trees.NewApi;

/**
 * Hello world!
 *
 */
public class MapFunc {
    public static void main(String[] args) {


        //******************************************MAP KULLANIMI***************************************************** */
        // System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\bin");

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

        newRdd.foreach(new VoidFunction<CupModel>() {
        @Override
        public void call(CupModel cupModel) throws Exception {
        System.out.println(cupModel.yil+" " +cupModel.getEvSahibi());
        }
        });


       //**********************************************FİLTER KULLANIMI*****************************************************/
        JavaRDD<CupModel> toplamGol = newRdd.filter(new Function<CupModel, Boolean>() {
            @Override
            public Boolean call(CupModel cupModel) throws Exception {
             //   return cupModel.birinci.equals("Italy");
             return cupModel.getToplamGol()<120;

            }
        });

        toplamGol.foreach(new VoidFunction<CupModel>() {
            @Override
            public void call(CupModel cupModel) throws Exception {
               System.out.println(cupModel.getYil()+"yilinda toplam gol sayisi="+ cupModel.getToplamGol());
                
            }
            
        });

    }

}
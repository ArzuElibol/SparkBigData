package com.example;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ders4 {
    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession.builder().appName("FirstSql").master("local").getOrCreate();
        Dataset<Row> rawData = sparkSession.read().json("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\sparkSql\\sparksql\\book.json");  
        rawData.createOrReplaceTempView("book");
        Dataset<Row> sql = sparkSession.sql("select language, pages from book ");
       
        sql.show();
   // rawData.createOrReplaceGlobalTempView(null); ekip çalışmlarında bizim view ımızn diğerlerince görünmesi için global kullanılırı
   
   
    }
    
}

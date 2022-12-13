package com.example;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ders3 {
    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession.builder().appName("FirstSql").master("local").getOrCreate();
        Dataset<Row> rawData = sparkSession.read().json("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\sparkSql\\sparksql\\book.json");  
   Dataset<Row> languageData = rawData.groupBy(new Column("language").equalTo("Italian")).count();
   Dataset<Row> pagesAvgData = rawData.groupBy(new Column("language")).avg("pages");
   pagesAvgData.show();
        rawData.groupBy(new Column("author")).sum("pages").show();
   
    }
}

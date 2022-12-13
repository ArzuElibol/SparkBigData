package com.example;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ders2 {
    public static void main(String[] args) {
                
SparkSession sparkSession=SparkSession.builder().appName("FirstSql").master("local").getOrCreate();

Dataset<Row> data = sparkSession.read().option("multiline",true).json("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\sparkSql\\sparksql\\show.json");
    data.show();
Dataset<Row> comedyData = data.filter(new Column("MAIN_GENRE").equalTo("comedy"));
    comedyData.show();
Dataset<Row> containsData = data.filter(new Column("SCORE").contains("7"));
    containsData.show();

    }
}

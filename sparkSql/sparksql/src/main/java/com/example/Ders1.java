package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class Ders1 
{
    public static void main( String[] args )
    {
        
SparkSession sparkSession=SparkSession.builder().appName("FirstSql").master("local").getOrCreate();
Dataset<Row> data = sparkSession.read().json("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\sparkSql\\sparksql\\book.json");
        data.show();
//data.printSchema();//başlıkların tiplerini gösteriyor
Dataset groupByData = data.groupBy("language").sum("pages");
        groupByData.show();
//Dataset<Row> authorPageData = data.select("author","pages");
        //authorPageData.show();
        
    }
}

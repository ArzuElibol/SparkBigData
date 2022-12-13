package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Distinct {

    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession.builder().master("local").appName("ReadCsv").getOrCreate();
    String filePath=("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\ProgrammerZone\\demo\\src\\resources\\movie.csv");
    Dataset<Row> dataset = sparkSession.sqlContext().read()
            .format("com.databricks.spark.csv")
            .option("header", true)
            .load(filePath);
            dataset.show(false);
     dataset.distinct().show();
    }
   
    

         



}

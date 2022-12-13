package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class ReadCsv 
{
    public static void main( String[] args )
    {
        SparkSession sparkSession=SparkSession.builder().master("local").appName("ReadCsv").getOrCreate();
//String filePath=ReadCsv.class.getResource("movie.csv").getPath();
String filePath=("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\ProgrammerZone\\demo\\src\\resources\\movie.csv");

Dataset<Row> dataset = sparkSession.sqlContext().read().format("com.databricks.spark.csv")
            .option("header", true)
            //Apache Spark with Java to read semi-structured file with Special Character separated values
           // .option("delimiter", "#")
            .load(filePath);
dataset.show();
//dataset.show(false); truncate false dersen eğer bircolumnda birden fazla aynı tekrar eden veri varsa onalrı gösterir

    }
}

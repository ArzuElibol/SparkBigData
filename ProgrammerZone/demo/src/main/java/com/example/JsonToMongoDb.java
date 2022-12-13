package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.mongodb.spark.MongoSpark;

public class JsonToMongoDb {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
        .master("local")
        .appName("MongoSparkJsonConn")
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.JsonDb")
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.JsonDb")
        .config("spark.mongodb.output.database","test")
        .config("spark.mongodb.output.collection","JsonData")
        .getOrCreate();

         String filePath=("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\ProgrammerZone\\demo\\src\\resources\\customer.json");

Dataset<Row>ds=sparkSession.read()
            .option("multiline", true)
            .json(filePath);

// ds.write().format("com.mongodb.sql.DefaultSource")
//             .option("database","jsonDb")
//             .option("collection", "jsonData")
//             .mode("append").save();
 MongoSpark.save(ds);

    }

}

package com.example;




import java.io.File;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ders5 {


    public static void main(String[] args) {



    SparkSession sparkSession=SparkSession.builder().appName("FirstSql").master("local").getOrCreate();
        
        Encoder<BookModel> bookEncoder =Encoders.bean(BookModel.class);
        Dataset<BookModel> data = sparkSession.read()
            .json("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\sparkSql\\sparksql\\book.json").as(bookEncoder);  


 data.select("name","age").show();
    //     data.foreach(x -> {System.out.println(x.getAuthor()+" :"+ x.getCountry());
    // }); 

    }
}

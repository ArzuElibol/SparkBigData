package com.example;




import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.example.model.Root;

public class ReadJsonObje{
    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession.builder().master("local").appName("ReadCsv").getOrCreate();
        // String filePath=ReadCsv.class.getResource("book.json").getPath();
        String filePath=("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\ProgrammerZone\\demo\\src\\resources\\customer.json");
   Dataset<Row> dataset = sparkSession.read()
        .option("multiline", true)
        .json(filePath);


Encoder<Root>obj=Encoders.bean(Root.class);
Dataset<Root> datasetObj = sparkSession.read()
.option("multiline",true)
.json(filePath)
.as(obj);
   
datasetObj.printSchema();
datasetObj.show(false);
 // dataset.show(false); //false ekledik ki columda görünmeyen alanlar görünsün
        
    }

}

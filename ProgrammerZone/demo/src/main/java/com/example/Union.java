package com.example;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Union {
    public static void main(String[] args) {
          // JavaSparkContext sc = new JavaSparkContext("local", "map func");
    // JavaRDD<String> RawData = sc.textFile("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\sparkCore\\WorldCups.csv");
    // JavaRDD<String> seconRdd = sc
    //         .textFile("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\sparkCore\\WorldCupPlayers.csv");
    // JavaRDD<String> resultRdd = RawData.union(seconRdd);

    // System.out.println("1.rdd" + RawData.count());
    // System.out.println("2.Rdd" + seconRdd.count());
    // System.out.println("3.Rdd" + resultRdd.count());

    SparkSession sparkSession=SparkSession.builder().master("local").appName("ReadCsv").getOrCreate();

    String filePath=("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\ProgrammerZone\\demo\\src\\resources\\WorldCupPlayers.csv");
    String filePath2=("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\ProgrammerZone\\demo\\src\\resources\\WorldCups.csv");
   

    Dataset<Row> dataset = sparkSession.read().format("com.databricks.spark.csv").option("header", true).load(filePath);
    Dataset<Row> dataset2 = sparkSession.read().format("com.databricks.spark.csv").option("header", true).load(filePath2);



//hata veriyor column sayısı eşit değil 

//   Dataset<Row> unionFunction= dataset.union(dataset2);
//    unionFunction.show();



   /*******************istediğin column görünmesin */
   Dataset<Row> single= dataset2.drop("QualifiedTeams");
   single.printSchema();
   single.show();


   Dataset<Row> multiple= dataset2.drop("QualifiedTeams","country","year");
   multiple.printSchema();
   multiple.show();




    }
   
  
}

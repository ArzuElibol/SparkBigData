package com.bigdata.map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Hello world!
 *
 */
public class IlkOrnek 
{
    public static void main( String[] args )
    {
               
        JavaSparkContext cont = new JavaSparkContext("local", "First Exam");
        JavaRDD<String> wordRdd = cont.textFile("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\rddmap\\ilkDatatext.txt");
System.out.println("ilk değer:"+ wordRdd.first());
    }
}

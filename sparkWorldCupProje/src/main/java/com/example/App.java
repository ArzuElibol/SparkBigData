package com.example;

import java.util.Iterator;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;

import com.google.common.collect.Iterators;
import com.mongodb.spark.MongoSpark;

import scala.Tuple2;


public class App {
    public static void main(String[] args) {

        // verileri mongodb ye yazmak için session oluşturuyoryuz.
        SparkSession spark = SparkSession.builder()

        .master("local")
        .appName("MongoSparkConnectorIntro")
        .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.WorldCupCollection")
        .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.WorldCupCollection")
        .config("spark.mongodb.output.database","test")
        .config("spark.mongodb.output.collection","myCollection")
        .getOrCreate();


        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        
           JavaRDD<String> RawData = sc.textFile("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\sparkWorldCupProje\\WorldCupPlayers.csv");

        System.out.println(RawData.count());

        JavaRDD<PlayersModel> playersRdd = RawData.map(new Function<String, PlayersModel>() {

            @Override
            public PlayersModel call(String v1) throws Exception {
                // regex e -1 ekleyince csv nin ilk satırını headerı atladığı için hata vermiyor
                String[] dizi = v1.split(",", -1);
                return new PlayersModel(dizi[0], dizi[1], dizi[2], dizi[3], dizi[4], dizi[5], dizi[6], dizi[7],
                        dizi[8]);
            }

        });

        JavaRDD<PlayersModel> Tur = playersRdd.filter(new Function<PlayersModel, Boolean>() {

            @Override
            public Boolean call(PlayersModel v1) throws Exception {
                return v1.getTeamInitials().equals("TUR");
            }

        });

        // playersRdd.foreach(new VoidFunction<PlayersModel>() {
        // @Override
        // public void call(PlayersModel playersModel) throws Exception {
        // System.out.println(playersModel.getPlayerName());
        // }
        // });

        // JavaRDD<PlayersModel> MessiRdd = playersRdd.filter(new
        // Function<PlayersModel,Boolean>() {
        // @Override
        // public Boolean call(PlayersModel v1) throws Exception {
        // return v1.getPlayerName().equals("MESSI");
        // }
        // });
        // System.out.println("MEssi dünya kupalarında " + MessiRdd.count() + " maç
        // yaptı");

        JavaPairRDD<String, String> mapToPair = Tur.mapToPair(new PairFunction<PlayersModel, String, String>() {

            @Override
            public Tuple2<String, String> call(PlayersModel playersModel) throws Exception {
                return new Tuple2<String, String>(playersModel.getPlayerName(), playersModel.getMatchID());
            }
        });
        // mapToPair.foreach(new VoidFunction<Tuple2<String, String>>() {

        // @Override
        // public void call(Tuple2<String, String> line) throws Exception {
        // System.out.println(line._1()+" "+ line._2());
        // }

        // });
        JavaPairRDD<String, Iterable<String>> playersMatch = mapToPair.groupByKey();
        // playersMatch.foreach(new VoidFunction<Tuple2<String,Iterable<String>>>() {
        // @Override
        // public void call(Tuple2<String, Iterable<String>> t) throws Exception {
        // System.out.println(t._1()+" "+t._2());
        // }
        // });

        JavaRDD<GroupPlayersModel> resultRdd = playersMatch
                .map(new Function<Tuple2<String, Iterable<String>>, GroupPlayersModel>() {

                    @Override
                    public GroupPlayersModel call(Tuple2<String, Iterable<String>> dizi) throws Exception {
                        Iterator<String> iteratorRow = dizi._2().iterator();
                        int size = Iterators.size(iteratorRow);
                        return new GroupPlayersModel(dizi._1(), size);
                    }

                });

        // resultRdd.foreach(new VoidFunction<GroupPlayersModel>() {
        // @Override
        // public void call(GroupPlayersModel t) throws Exception {
        // System.out.println(t.getPlayerName()+" "+ t.getMatchCount());
        // }
        // });

        /**
         * Mongodb ye yazıdırırken verilerin JSON formatında olması lazım. bizim şu anda
         * verilerimiz GroupPlayer obje formatında
         * verileri JSON olarak döndürmek için döküman tipinde döndürmemiz gerekiyor.
         * Bunun için ;
         */
        JavaRDD<Document> MongoRdd = resultRdd.map(new Function<GroupPlayersModel, Document>() {
            // import org.bson.Document;
            @Override
            public Document call(GroupPlayersModel v1) throws Exception {
                return Document.parse("{PlayerName: " + "'" + v1.getPlayerName()
                        + "'"
                        + ","
                        + "MAtchCount: "+"'" + v1.getMatchCount()+"'"+"}");
            }

        
        });

        MongoSpark.save(MongoRdd);

    }

}
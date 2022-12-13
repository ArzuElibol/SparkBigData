package com.example;




import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import com.example.model.Movie;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;



public class MongoDbConn {
    public static void main(String[] args) {
        SparkSession sparkSession=SparkSession.builder()
        .appName("MongoDbDeneme")
        .master("local")
    //.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.myCollection")
       .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.movieCollection")
       .config("spark.mongodb.output.database","test")
       .config("spark.mongodb.output.collection","movieCollection")
        .getOrCreate();

    
        JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext());
    //   readData(sc);
       writeData(sc);
    }

    private static void writeData(JavaSparkContext sc) {

         JavaRDD<String> RawData = sc.textFile
        ("C:\\Users\\SIBER2021\\Desktop\\SparkBigData\\ProgrammerZone\\demo\\src\\resources\\movie.csv");

        JavaRDD<Movie> movieRdd = RawData.map(new Function<String, Movie>() {

            @Override
            public Movie call(String v1) throws Exception {
                // regex e -1 ekleyince csv nin ilk satırını headerı atladığı için hata vermiyor
                String[] dizi = v1.split(",", -1);
                return new Movie(dizi[1], dizi[4]);
            }
        });

       movieRdd.foreach(new VoidFunction<Movie>() {
        @Override
        public void call(Movie movie) throws Exception {
        System.out.println(movie.getTitle()+" "+movie.getMainGenre());
        }
        });

JavaRDD<Document> MongoRdd = movieRdd.map(new Function<Movie, Document>() {
    // import org.bson.Document;
    @Override
    public Document call(Movie v1) throws Exception {
        return Document.parse("{MovieTitle: " + "'" + v1.getTitle()
        + "'"
        + ","
        + "Genre: " +"'"+ v1.getMainGenre()+"'"+"}");
    }

    /* {
    "movieTitle": "The White Stripes",
    "mainGenre": 1999,
     }, */

});

WriteConfig writeConfig=WriteConfig.create(sc);
MongoSpark.save(MongoRdd,writeConfig);


}

    private static void readData(JavaSparkContext sc) {

        JavaMongoRDD<Document>rdd=MongoSpark.load(sc);
        rdd.toDF().show(false);
        System.out.println("Toplam kayit: "+rdd.count());

//         ReadConfig readConfig = ReadConfig.create(sparkContext)
//                 .withOption("spark.mongodb.output.collection", "myCollection");
// JavaRDD<Document> rdd = MongoSpark.load(sparkContext, readConfig);
 
}
}
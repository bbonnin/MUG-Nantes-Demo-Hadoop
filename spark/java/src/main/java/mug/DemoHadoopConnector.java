package mug;

import org.apache.commons.math3.util.DefaultTransformer;
import org.apache.commons.math3.util.NumberTransformer;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.BSONObject;

import scala.Tuple2;

import com.mongodb.BasicDBObject;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;

/**
 * Demo du connecteur Hadoop de MongoDB.
 *
 * @author Bruno
 *
 */
public class DemoHadoopConnector {

    private static final NumberTransformer NT = new DefaultTransformer();

    @SuppressWarnings({ "resource", "rawtypes", "unchecked" })
    public static void main(String[] args) {



        final SparkConf conf = new SparkConf()
        .setAppName("mug-demo-hadoop-connector")
        .setMaster("local[2]");

        final JavaSparkContext sc = new JavaSparkContext(conf);

        // Configuration pour les lectures/écritures dans MongoDB
        //
        final Configuration inputConfig = new Configuration();
        inputConfig.set("mongo.input.uri", "mongodb://localhost:27017/marketdata.minbars");

        final Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/marketdata.maxminbars");

        // Dataset des données brutes de MongoDB
        //
        final JavaPairRDD<Object, BSONObject> rawDataRDD = sc.newAPIHadoopRDD(inputConfig,
                MongoInputFormat.class, // Input format
                Object.class,           // Key
                BSONObject.class);      // Value

        final JavaRDD<BSONObject> data = rawDataRDD.values();

        // Dataset des données groupées par (Symbol, Day)
        //
        final JavaPairRDD<Object, Iterable<BSONObject>> groupbyRdd = data.groupBy(doc -> new Tuple2(doc.get("Symbol"), doc.get("Day")));

        // Dataset des données agrégées
        //
        final JavaPairRDD<Object, DbObject> aggRdd = groupbyRdd
            .aggregateByKey(new DbObject(),
                (dbObj, docs) -> {
                    for (BSONObject obj : docs) {
                        dbObj.processHighAndLow(obj);
                    }
                    return dbObj;
                },
                (dbObj1, dbObj2) -> {
                    final DbObject dbObj =  new DbObject();
                    dbObj.processHighAndLow(dbObj2);
                    return dbObj;
                })
                ;



        // Stockage dans MongoDB
        //
        aggRdd
            .saveAsNewAPIHadoopFile("file:///this-is-completely-unused",
                Object.class,
                BSONObject.class,
                MongoOutputFormat.class,
                outputConfig);

//        for (Entry<Object, DbObject> entry : aggRdd.collectAsMap().entrySet()) {
//            System.out.println(entry.getKey() + " => " + entry.getValue());
//        }

    }

    static class DbObject extends BasicDBObject {

        public void processHighAndLow(final BSONObject obj) {
            put("High", get("High") == null ? obj.get("High") : Math.max(NT.transform(get("High")), NT.transform(obj.get("High"))));
            put("Low", get("Low") == null ? obj.get("Low") : Math.max(NT.transform(get("Low")), NT.transform(obj.get("Low"))));

            // Récupération de Symbol et Day
            if (get("Symbol") == null || get("Day") == null) {
                put("Symbol", obj.get("Symbol"));
                put("Day", obj.get("Day"));
            }
        }
    }

}

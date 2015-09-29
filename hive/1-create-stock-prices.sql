ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-java-driver-3.0.3.jar;
ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-hadoop-core-1.4.0.jar;
ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-hadoop-hive-1.4.0.jar;

DROP TABLE IF EXISTS stock_prices;

CREATE EXTERNAL TABLE stock_prices
(
    id STRUCT<oid:STRING, bsontype:INT>,
    symbol STRING,
    timestamp STRING,
    day INT,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume INT
)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES (
    'mongo.columns.mapping'='{"id":"_id", "symbol":"Symbol", "timestamp":"Timestamp", "day":"Day", "open":"Open", "high":"High", "low":"Low", "close":"Close", "volume":"Volume"}'
)
TBLPROPERTIES(
    'mongo.uri'='mongodb://localhost:27017/marketdata.stock_prices'
);


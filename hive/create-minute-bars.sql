ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-java-driver-3.0.3.jar;
ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-hadoop-core-1.4.0.jar;
ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-hadoop-hive-1.4.0.jar;

DROP TABLE IF EXISTS minute_bars;

CREATE EXTERNAL TABLE minute_bars
(
    id STRUCT<oid:STRING, bsontype:INT>,
    Symbol STRING,
    Timestamp STRING,
    Day INT,
    Open DOUBLE,
    High DOUBLE,
    Low DOUBLE,
    Close DOUBLE,
    Volume INT
)
STORED BY 'com.mongodb.hadoop.hive.MongoStorageHandler'
WITH SERDEPROPERTIES (
    'mongo.columns.mapping'='{"id":"_id", "Symbol":"Symbol", "Timestamp":"Timestamp", "Day":"Day", "Open":"Open", "High":"High", "Low":"Low", "Close":"Close", "Volume":"Volume"}'
)
TBLPROPERTIES(
    'mongo.uri'='mongodb://localhost:27017/marketdata.minbars'
);


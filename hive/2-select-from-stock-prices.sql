ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-java-driver-3.0.3.jar;
ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-hadoop-core-1.4.0.jar;
ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-hadoop-hive-1.4.0.jar;

SELECT symbol, timestamp, close, volume FROM stock_prices LIMIT 10;

--    id STRUCT<oid:STRING, bsontype:INT>,
--    symbol STRING,
--    timestamp STRING,
--    day INT,
--    open DOUBLE,
--    high DOUBLE,
--    low DOUBLE,
--    close DOUBLE,
--    volume INT


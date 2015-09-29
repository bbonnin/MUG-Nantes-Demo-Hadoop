ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-java-driver-3.0.3.jar;
ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-hadoop-core-1.4.0.jar;
ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-hadoop-hive-1.4.0.jar;

SELECT Symbol, Timestamp, Close, Volume from minute_bars LIMIT 10;

--    id STRUCT<oid:STRING, bsontype:INT>,
--    Symbol STRING,
--    Timestamp STRING,
--    Day INT,
--    Open DOUBLE,
--    High DOUBLE,
--    Low DOUBLE,
--    Close DOUBLE,
--    Volume INT


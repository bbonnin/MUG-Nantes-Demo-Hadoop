ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-java-driver-3.0.3.jar;
ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-hadoop-core-1.4.0.jar;
ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-hadoop-hive-1.4.0.jar;

INSERT INTO TABLE max_min_minute_bars
    SELECT maxmin.Symbol, maxmin.Day, maxmin.MaxHigh, maxmin.MinLow FROM
    (
        SELECT Symbol, Day, MAX(High) as MaxHigh, MIN(Low) as MinLow FROM minute_bars GROUP BY Symbol, Day
    )
    as maxmin;


--    id STRUCT<oid:STRING, bsontype:INT>
--    Symbol STRING,
--    Timestamp STRING,
--    Day INT,
--    Open DOUBLE,
--    High DOUBLE,
--    Low DOUBLE,
--    Close DOUBLE,
--    Volume INT


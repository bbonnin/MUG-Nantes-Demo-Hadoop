ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-java-driver-3.0.3.jar;
ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-hadoop-core-1.4.0.jar;
ADD JAR /home/cloudera/MUG-Nantes-Demo-Hadoop/mongodb/libs/mongo-hadoop-hive-1.4.0.jar;


SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;


INSERT INTO TABLE max_min_prices_by_day
    SELECT p.symbol, c.name, p.day, MAX(p.high), MIN(p.low)
    FROM stock_prices p, company c 
    WHERE c.symbol = p.symbol 
    GROUP BY p.symbol, c.name, p.day
;



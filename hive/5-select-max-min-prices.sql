
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;

SELECT company, day, maxHigh, minLow FROM max_min_prices_by_day;


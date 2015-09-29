
DROP TABLE IF EXISTS max_min_prices_by_day;

CREATE TABLE max_min_prices_by_day
( 
    symbol STRING,
    company STRING,
    day INT,
    maxHigh DOUBLE,
    minLow DOUBLE
);


# MUG-Nantes-Demo-Hadoop
Demo  connecteur Hadoop de MongoDB

## Etape 0 - Build
- Java
```bash
cd spark/java
mvn clean package assembly:single
```


## Etape 1 - Import data
- Clean (mongo shell)
```bash
use marketdata
db.stock_prices.drop()
```
- Source : cf. http://www.barchartmarketdata.com/data-samples/mstf.csv
- Import
```bash
mongoimport nom_fichier.csv --type csv --headerline -d marketdata -c stock_prices
```
- Data des sociétés (fichier texte mis dans HDFS)
```bash
data/put-hdfs.sh
```

## Etape 2 - Hive demo
- Création table des sociétés
```bash
hive -f hive/0-create-company.sql
```
- Création table externe
```bash
hive -f hive/1-create-stock-prices.sql
```
- Select sur la nouvelle table
```bash
hive -f hive/2-select-from-stock-prices.sql
```
- Création table des max/min
```bash
hive -f hive/3-create-max-min-prices.sql
```
- Insertion des données dans table des max/min
```bash
hive -f hive/4-insert-max-min-prices.sql
```
- Select dans table des max/min
```bash
hive -f hive/5-select-max-min-prices.sql
```

## Etape 3 - Spark demo
- Clean (mongo shell)
```bash
use marketdata
db.max_min_prices.drop()
```
- Lancement tâche Spark
```bash
spark/run-java-connector-demo.sh
```
- Check data (mongo shell)
```bash
use marketdata
db.max_min_prices.find().sort({"Day":1})
```

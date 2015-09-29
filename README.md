# MUG-Nantes-Demo-Hadoop
Demo  connecteur Hadoop de MongoDB

## Build Java

```bash
cd spark/java
mvn clean package assembly:single
```

## Import data
- Cf. http://www.barchartmarketdata.com/data-samples/mstf.csv
- Import
```bash
mongoimport nom_fichier.csv --type csv --headerline -d marketdata -c stock_prices
```


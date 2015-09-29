import math
from pyspark import SparkContext


#sc = SparkContext("spark://quickstart.cloudera:7077", "Test MongoDB Connector")
sc = SparkContext("local", "Test MongoDB Connector")


# Config MongoDB
inputConfig = { "mongo.input.uri" : "mongodb://localhost:27017/marketdata.stock_prices" }
outputConfig = { "mongo.output.uri" : "mongodb://localhost:27017/marketdata.maxminprices" }

# Config pour RDD qui va lire les data dans MongoDB
inputFormatClassName = "com.mongodb.hadoop.MongoInputFormat"
keyClassName = "java.lang.Object"
valueClassName = "org.bson.BSONObject"

stockPricesRDD = sc.newAPIHadoopRDD(inputFormatClassName, keyClassName, valueClassName, None, None, inputConfig)

# Config pour RDD qui va ecrire dans MongoDB
outputFormatClassName = "com.mongodb.hadoop.MongoOutputFormat"

# Les traitements...
# ... sur l'ensemble des data
prices = stockPricesRDD.values()

# ... groupby sur (symbol, day)
groupByRDD = prices.groupBy(lambda doc: (doc["Symbol"], doc["Day"]))
#                  .map(lambda tuple: (tuple[0], tuple[1])) \
#                  .collect()

# ... aggregate par clef (on prend le max de High et le min de Low)
def maxMin(doc, groupedDocs):
    for gdoc in groupedDocs:
        doc = { "High" : max(doc.get("High", 0), gdoc["High"]), "Low" : min(doc.get("Low", 99999), gdoc["Low"]) }
    return doc

aggRDD = groupByRDD.aggregateByKey(
    { },
    maxMin,
    lambda doc1, doc2: { "High" : max(doc1["High"], doc2["High"]), "Low" : min(doc1["Low"], doc2["Low"])})

# ... sauve dans HDFS
aggRDD.saveAsTextFile("hdfs:///user/cloudera/data/spark_result")
 
# ... collecte du tout
#result = aggRDD.collect()

#for doc in result:
#    print doc[0], doc[1]


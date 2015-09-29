import math
from pyspark import SparkContext

#sc = SparkContext("spark://quickstart.cloudera:7077", "Test MongoDB Connector")
sc = SparkContext("local", "Test MongoDB Connector")


# Config MongoDB
config = { "mongo.input.uri" : "mongodb://localhost:27017/marketdata.minbars", "mongo.output.uri" : "mongodb://localhost:27017/marketdata.maxminbars" }

# Config pour RDD qui va lire les data dans MongoDB
inputFormatClassName = "com.mongodb.hadoop.MongoInputFormat"
keyClassName = "org.apache.hadoop.io.Text"
valueClassName = "org.apache.hadoop.io.MapWritable"

minBarRawRDD = sc.newAPIHadoopRDD(inputFormatClassName, keyClassName, valueClassName, None, None, config)

#config["mongo.output.uri"] = "mongodb://localhost:27017/marketdata.maxminbars"
# Config pour RDD qui va ecrire dans MongoDB
outputFromatClassName = "com.mongodb.hadoop.MongoOutputFormat"

# Les traitements...

def mm(doc1, doc2):
    print "OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO"
    print doc1
    print doc2
    print "oooooooooooooooooooooooooooooooooooooooooooooooooo"
    return doc1

# ... sur l'ensemble des data
#minBarRDD = minBarRawRDD.values()
minBarRDD = sc.parallelize([{"Symbol":"M","Day":1,"High":1}, {"Symbol":"M","Day":1,"High":2}, {"Symbol":"M","Day":2,"High":3}]) 

result = minBarRDD.groupBy(lambda doc: (doc["Symbol"], doc["Day"])) \
                  .reduceByKey(mm) \
                  .collect()

#dataSource = sc.parallelize( [("user1", "film"), ("user1", "film"), ("user2", "film"), ("user2", "books"), ("user2", "books")] )
#dataReduced = dataSource.reduceByKey(lambda x,y : x + "," + y)
#result = dataReduced.map(lambda (user,values) : (values.split(","),user))


# ... groupby sur (symbol, day)
#groupByRDD = minBarRDD.groupBy(lambda doc: (doc["Symbol"], doc["Day"]))

# ... reduce par clef (on prend le max de High et le min de Low)
#reduceRDD = groupByRDD.reduceByKey(lambda doc1, doc2: { "High1" : math.max(doc1["High"], doc2["High"]), "Low1" : math.min(doc1["Low"], doc2["Low"])})

#reduceRDD = groupByRDD.reduceByKey(mm)

# ... trie sur les clefs
#sortRDD = reduceRDD.sortByKey()

# ... collecte du tout
#result = sortRDD.collect()

#print "**************** count = %d" % len(result)

print result
for doc in result:
    print doc[0], doc[1]
    for v in doc[1]:
        print "   " , v
        print "************************************"

def maxMin(groupedSymbols):
    print "*******************************************************"
    for doc in groupedSymbols:
        print "doc %s" % doc
    outputDoc = { "Symbol" : groupedSymbols[0][0] }
    print "======================================================="
    return (None, outputDoc)

#resultRDD = symbols.map(maxMin)


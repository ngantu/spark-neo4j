## Use PageRank algorithm of GraphX to get the importance of each node
#import libraries
import findspark
findspark.init()
findspark.find()
import pyspark
findspark.find()
from graphframes import GraphFrame
from graphframes import *

# Read data from csv
sc =pyspark.SparkContext()
nodes_rdd = sc.textFile("H:\Data\speech_nodes.csv").map(lambda line: line.split(","))
nodes_df = nodes_rdd.toDF(['id','type'])
nodes_df.show()

edges_rdd = sc.textFile("H:\Data\speech_edges.csv").map(lambda line: line.split(","))
edges_df = edges_rdd.toDF(['src','dst'])
edges_df.show()

# Create a GraphFrame
g = GraphFrame(nodes_df, edges_df)

# Run PageRank algorithm, and show the importance of each node.
results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()
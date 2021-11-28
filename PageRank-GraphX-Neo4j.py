## Use PageRank algorithm of GraphX to get the importance of each node

from graphframes import *

# Cypher query to load dataset for nodes from Neo4j
nodes_qry = """ 
    MATCH(n) RETURN n.name as id
"""
# Save dataset for nodes to Spark's DataFrame
nodes_df = spark.read.format("org.neo4j.spark.DataSource") \
        .option(“url”, “neo4j://localhost:7687”) \
        .option("database", “neo4j”) \
        .option(“authentication.type”, “basic”) \
        .option("authentication.basic.username", "neo4j") \
        .option("authentication.basic.password", "password")\
        .option("query", nodes_qry) \
        .load()

       
nodes_df.show()

# Cypher query to load dataset for relationships from Neo4j
edges_qry = """ 
    MATCH p=(s)-[r:with]->(d) RETURN s.name as src, d.name as dst
"""
# Save dataset for relationships to Spark's DataFrame
edges_df = spark.read.format("org.neo4j.spark.DataSource") \
        .option(“url”, “neo4j://localhost:7687”) \
        .option("database", “neo4j”) \
        .option(“authentication.type”, “basic”) \
        .option("authentication.basic.username", "neo4j") \
        .option("authentication.basic.password", "password")\
        .option("query", edges_qry) \
        .load()

edges_df.show()

# Create a GraphFrame in Spark
g = GraphFrame(nodes_df, edges_df)

# Run PageRank algorithm, and show the importance of each node.
results = g.pageRank(resetProbability=0.01, maxIter=20)
results.vertices.select("id", "pagerank").show()

#//Sortby ranking and show top 20 rows 
from pyspark.sql.functions import *
results.vertices.select("id", "pagerank").orderBy(desc("pagerank")).show()


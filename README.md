# spark-neo4j
My first test: Build a data graph model from following two files:
- speech_nodes.csv file contains words (good, pleasant, glad, person,...) and their corresponding parts of speech (noun, adjective), 
- and the speech_edges.csv file contains connections between them.


I/ How to install and launch the code (import scripts, etc.)
I.1 Install steps to launch Spark and run code written in Jupyter Notebook module of Anaconda in Windows
  1. Install Java 8 (jre1.8.0_291, jdk-16.0.1)
  2. Install Python 3.8.5
  3. Install Anaconda (for python)
  4. Install Apache Spark (spark-3.0.2-bin-hadoop3.2)
  5. Configure Environment Variables (JAVA_HOME, SPARK_HOME, HADOOP_HOME)
I.2 Create a blank database in Neo4j Desktop 
  1. Install Neo4j Desktop (4.3)
  2. Launch Neo4j Desktop and create a blank database, then start this database to connect Spark 
II/ What is the property graph model (the transformation you made)
Display the property graph model as follows:
  1. Launch Jupyter Notebook from command line of Anaconda Prompt (miniconda3) 
  2. Open "neospark-demo.ipynb" file and run.
  3. After above step, then launch Neo4j Desktop
  4. The data graph model can be seen through Neo4j Browser of Neo4j Desktop application or the link http://localhost:7474/browser/ with the Cypher query
      "MATCH p=()-[r:with]->() RETURN p LIMIT 100"
  In Neo4j Browser interface, at Neo4j$ line type following command "MATCH p=()-[r:with]->() RETURN p LIMIT 100" to see 67 nodes and 100 relationships of the graph

III/ What are the treatments in graphX you've done (e.g: search for triangle, etc.) and why
This test will continue using PageRank algorithm of GraphX to get the importance of each node.

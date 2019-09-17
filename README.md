# Apache Spark & Kafka : Proof of Concepts
## RDD Use Cases : '.java' files under Project/src/main/java/com/virtualpairprogrammers
### 1) Word Count
Implementation of reading a file locally and from AWS S3 bucket along with AWS EMR (Elastic MapReduce) to get a word count from a huge file using Spark's distributed engine. The program also contains a helper class 'Util.java', which ignores all the stop words (ex: 'a', 'an', 'else', etc). This is an extra feature which is totally optional.

### 2) Online Educational Platform Course Views Analysis
Analyzing the number of views per chapter from number of students and rating each chapter accordingly. Implementation using RDD's.

### 3) SparkSQL examples
SparkSQL use cases show casing User Defined Functions in Spark.

Usage
-----    
> First download all the dependencies by running the 'pom.xml' under 'Project/src/target/pom.xml'.
> Then just run any '.java' files under 'Project/src/main/java/com/virtualpairprogrammers'
> All input files are already placed in the package.

## Machine Learning with SparkMlib
Implementation of 3 Supervised & 2 Unsupervised Machine Learning Algorithms:

Supervised             | Unsupervised
---------------------- | --------------------------
1. Linear Regression   | 1. K-Means
2. Logistic Regression | 2. Collaborative Filtering
3. Decision Trees      |

### Linear Regression
> Used when the values we want to predict have wide range of possible answers. 

1. Predicting the customers who might continue their subscription for watching the educational videos on the virtual pair programmers platform.

2. Predicting the number of repetitions a male/female might be able to do based on their height, age, weight and other physical parameters in the Gym Competitors dataset.

3. Predicting the house prices from the kaggel's famous 'kc_house_data' dataset. Also building an end to end Pipeline for the entire process.

4. Statistical ways used to measure model accuracy:
	1. Root Mean Squared Error.
	2. R square.

### Logistic Regression
> Used when the outcome we want to try and predict is a YES or NO type of solution.

Predicting the customers we need to filter out of our results who are unlikely to watch any educational videos and who might cancel their subscription.

### Decision Trees
> Used to generate a flowchart of results and the outcome possibilites are small.

Predicting which country has the most number of active users paying for the subscription of educational videos.

### K-Means Clustering
> Used to explore data and see what group of features can we use to run our prediction algorithms.

Predicting the clusters of male/female on the basis of their age, height, weight and other physcial parameters to see which section has the most number of repitions in them.

### Collaborative Filtering
> Also known as Matrix Factorization. Used to predict the missing entries in a dataset based on the historical data.

Used for course recommendation for users based on their past viewing history of the educational videos.

Usage
-----    
> First download all the dependencies by running the 'pom.xml' under 'SparkModule3/src/target/pom.xml'.
> Then just run any '.java' files under 'Project/src/main/java/com/virtualpairprogrammers'
> All input files are already placed in the package.

## Kafka, Spark DStreams and Structured Streaming
To run Kafka programs you will need a zookeeper and a kafka server. You can google or youtube how to download and install kafka. Zookeeper server comes with the kafka build.

Concepts implemented are:
1. Real time batch processing using Discretized stream.
2. Course Event log analysis with kafka.
3. Windows , Watermarks and data sinks in structred streaming.

Usage
-----    
There are 2 main sections for this part of the project.
1. For DStream version, refer to the 'LoggingServer.java' (which acts as the server) and 'LogStreamAnalysis.java' under the 'Project/src/main/java/com/virtualpairprogrammers/streaming' section. Re-run the 'pom.xml' if you haven't done so for the 'Project' where RDD concepts are implemented.
	
	* Start the server which is 'LoggingServer.java' and then in new console window run 'LogStreamAnalysis.java'

2. For Kafka and Structured streaming version, first start the zookeeper server in a console window. Once the port is binding, open up a fresh new console and run the kafka server. 

	1. Now open the 'viewing-figures-generation' project in your ide and run the 'pom.xml'.

	2. Now open up another fresh console and run the 'ViewReportsSimulator.java' which will act as our server. Keep the zookeeper and kafka servers on.

	3. Now open up another fresh console window and run either 'ViewingFiguresDStreamVersion.java' or 'ViewingFiguresStructuredStreamingVersion.java'.

	4. To stop your programs, first close either java programs mentioned in point 3, then stop the 'ViewReportsSimulator.java' server, then break the kakfka server using ctrl+c and then break the zookeeper server using ctrl+c.
    
## Acknowledgments
1. Udemy
2. Virtual Pair Programmers
3. Kaggle
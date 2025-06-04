The folder "Submission" Contains the Canva presentation we showed in class, 2 Jupyter notebooks (main one and a producer), the original CSV dataset 
we are using in the project and another synthetic CSV dataset we created for the presentation of real-time data streaming.


Setup Instructions:

Place the folder in Desktop in Linux (we used the VLAB), open the terminal from that folder and type: 

pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1

that's for testing parts 1 and 2.


For part 3 (real time), you need these commands:

1. cd /usr/local/kafka/kafka_2.13-3.2.1 ----> bin/zookeeper-server-start.sh config/zookeeper.properties
2. cd /usr/local/kafka/kafka_2.13-3.2.1 ----> bin/kafka-server-start.sh config/server.properties
3. cd /usr/local/kafka/kafka_2.13-3.2.1 ----> bin/kafka-topics.sh --create --topic depression-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1



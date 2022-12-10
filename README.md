Prerequisite -

You have to download the confluent 6.2.1 from the website.
And set the KAFKA_HOME env variable getting used in script to the path where you
have downloaded the confluent 6.2.1. e.g in my  machine it is set as 
(~/Downloads/confluent-6.2.1).

Kafka broker debugging notes :
1. When you want to produce or consume  the messages using 
   kafka-console-producer or consumer, the bootstrap-server that you should be
   specifying should not be localhost:9092 but the IP in the logs of
   kafka-server-start command and append 9092 to it.
   e.g. 192.168.1.3:9092
2. While terminating the kafka-brokers. First terminate the sessions of all the
   kafka-brokers and then zookeeper. Doing the other way around won't
   shut down the kafka-brokers.
3. The script consumer-start.sh if it does not work, modify the script
   by replacing localhost:9092 with the one found in any of the kafka-server-start
   logs.

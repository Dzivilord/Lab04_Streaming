# Lab 04 - Spark Streaming

## Description

This project implements a real-time data pipeline for processing time-series cryptocurrency prices using Apache Kafka and Apache Spark Structured Streaming. It consists of:

Extract: A Kafka producer fetches BTCUSDT prices from the Binance API and publishes enriched data with event-time to the topic btc-price.

Transform: Spark streaming jobs compute moving statistics (mean, std), Z-scores, and identify shortest windows of higher/lower price movements, publishing results to corresponding Kafka topics.

Load: Processed Z-score data is stored into MongoDB collections for persistent storage.

Bonus: Implements stateful logic to calculate shortest future windows of positive or negative price movements within a 20-second interval.

The pipeline handles late data (up to 10 seconds) and uses event-time semantics with sliding windows.

## Getting started

### Prerequisites

- Python: 3.10
- Spark: 3.5.1
- Java: 11
- Kafka: kafka_2.13-3.9.1

### Installation

Create and activate a virtual environment (optional but recommended)

```cmd
python -m venv venv
source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
```

Install dependencies

```cmd
pip install -r requirements.txt
```

## How to run programme

1.  Extract
- Go to Kafka Folder
- Open first terminal - Run ZooKeeper.
```cmd
bin/zookeeper-server-start.sh config/zookeeper.properties
```

- Open second terminal - Run Kafka.
```cmd
bin/kafka-server-start.sh config/server.properties
```

- Go to Load folder.
- Open terminal
```cmd
python3 CQ05.py
```

2.  Transform
- Make sure Kafka is running.
- Go to Transform folder.
- Open terminal - Run Moving program.
```cmd
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 CQ05_moving.py
```

- Open another terminal - Run Zscore program.
```cmd
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 CQ05_zscore.py
```

3.  Load
- Make sure Kafka is running.
- Go to Load folder.
- Open terminal.
```cmd
spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 CQ05.py
```


4.  Bonus

- Open first terminal- Where you store kafka

```cmd
cd C:/Kafka
python .src/Bonus/CQ05.py
.\bin\windows\kafka-server-start.bat .\config\kraft\server.properties
    or 
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

 .\bin\windows\kafka-server-start.bat .\config\server.properties
```

- Open second terminal
```cmd
cd Lab04_streaming

python .\src\Extract\CQ05.py
```
- Open third terminal
```cmd
Remove-Item -Path .\src\Bonus\main_checkpoint -Recurse -Force

cd Lab04_streaming

python .\src\Bonus\CQ05.py
```
- Open fourth terminal- Where you store kafka

```cmd
cd C:/Kafka
.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic btc-price-higher --from-beginning

.\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic btc-price-lower --from-beginning
```

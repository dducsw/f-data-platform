# Kafka with Debezium

## 1. Connect to Debezium

```
-- Connect to finance_db
\c finance_db;

-- Create debezium role if not exists
CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD '6666';

-- Grant permissions
GRANT CONNECT ON DATABASE finance_db TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;

-- Create publication for users and cards tables
CREATE PUBLICATION finance_pub FOR TABLE users, cards;
```

## 2. Create a topic
```
curl -X POST -H "Content-Type: application/json" --data @/usr/local/kafka/connectors/pg-finance-connector.json http://localhost:8083/connectors


```

## 3. Check the statuc
```
$ curl -s http://localhost:curl -s http://localhost:8083/connectors/debezium-finance-connector/status | jq
{
  "name": "debezium-finance-connector",
  "connector": {
    "state": "RUNNING",
    "worker_id": "127.0.1.1:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "127.0.1.1:8083"
    }
  ],
  "type": "source"
}
kafka@ldduc:/usr/local/kafka/connectors$ 


```


```
$ bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep finance
```
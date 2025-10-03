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
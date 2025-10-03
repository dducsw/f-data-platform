#!/bin/bash

# Kafka Topics Initialization Script for Debezium
# This script creates the necessary Kafka topics for Debezium CDC

# Kafka configuration
KAFKA_BROKER="localhost:9092"
ZOOKEEPER="localhost:2181"
REPLICATION_FACTOR=1
PARTITIONS=3

# Topic names
CONNECT_CONFIGS="connect-configs"
CONNECT_OFFSETS="connect-offsets"
CONNECT_STATUS="connect-status"
SCHEMA_CHANGES="schema-changes.inventory"
TRANSACTION_TOPIC="dbserver1.inventory.transactions"
CUSTOMER_TOPIC="dbserver1.inventory.customers"
PRODUCT_TOPIC="dbserver1.inventory.products"

echo "Creating Kafka topics for Debezium..."

# Create Connect framework topics
kafka-topics.sh --create \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor $REPLICATION_FACTOR \
    --partitions 1 \
    --topic $CONNECT_CONFIGS \
    --config cleanup.policy=compact

kafka-topics.sh --create \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor $REPLICATION_FACTOR \
    --partitions 25 \
    --topic $CONNECT_OFFSETS \
    --config cleanup.policy=compact

kafka-topics.sh --create \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor $REPLICATION_FACTOR \
    --partitions 5 \
    --topic $CONNECT_STATUS \
    --config cleanup.policy=compact

# Create application-specific topics
kafka-topics.sh --create \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor $REPLICATION_FACTOR \
    --partitions 1 \
    --topic $SCHEMA_CHANGES

kafka-topics.sh --create \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor $REPLICATION_FACTOR \
    --partitions $PARTITIONS \
    --topic $TRANSACTION_TOPIC

kafka-topics.sh --create \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor $REPLICATION_FACTOR \
    --partitions $PARTITIONS \
    --topic $CUSTOMER_TOPIC

kafka-topics.sh --create \
    --bootstrap-server $KAFKA_BROKER \
    --replication-factor $REPLICATION_FACTOR \
    --partitions $PARTITIONS \
    --topic $PRODUCT_TOPIC

echo "Listing created topics:"
kafka-topics.sh --list --bootstrap-server $KAFKA_BROKER

echo "Kafka topics initialization completed!"
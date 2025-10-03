#!/bin/bash

DB_HOST="localhost"
DB_USER="postgres"
DB_PASSWORD="6666"
DB_NAME="finance_db"
DATA_DIR="/home/ldduc/D/f-data-platform/data/postgres"
CSV_DIR="/home/ldduc/D/f-data-platform/data/csv"

export PGPASSWORD="$DB_PASSWORD"

# Check PostgreSQL connection
psql -h "$DB_HOST" -U "$DB_USER" -c "SELECT 1;" > /dev/null 2>&1 || {
    echo "[ERROR] Cannot connect to PostgreSQL server."
    exit 1
}

# Check required files
for f in "$DATA_DIR/ddl.sql" "$DATA_DIR/load_dataset.sql"; do
    [ -f "$f" ] || { echo "[ERROR] File not found: $f"; exit 1; }
done

# Check CSV files with correct names from load_dataset.sql
for f in "users_data.csv" "cards_data.csv" "transactions_data.csv"; do
    [ -f "$CSV_DIR/$f" ] || echo "[WARNING] CSV file not found: $CSV_DIR/$f"
done

# Create database and tables
psql -h "$DB_HOST" -U "$DB_USER" -f "$DATA_DIR/ddl.sql" || {
    echo "[ERROR] Failed to create database/tables."
    exit 1
}

# Load data
psql -h "$DB_HOST" -U "$DB_USER" -f "$DATA_DIR/load_dataset.sql" || {
    echo "[ERROR] Failed to load data."
    exit 1
}

# Verify data
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "
SELECT 'users: ' || COUNT(*) FROM users;
SELECT 'cards: ' || COUNT(*) FROM cards;
SELECT 'transactions: ' || COUNT(*) FROM transactions;
" || echo "[WARNING] Could not verify data."

echo "[SUCCESS] Setup completed!"
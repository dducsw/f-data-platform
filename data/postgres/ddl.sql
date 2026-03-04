-- Finance Transaction OLTP Database

create database finance_db;

\c finance_db;


drop table if exists customers;
drop table if exists cards;
drop table if exists merchants;
drop table if exists transactions;

-- 1. Customers
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    ssn VARCHAR(15),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender VARCHAR(10),
    street TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    zip VARCHAR(20),
    lat DECIMAL(10, 8),
    long DECIMAL(11, 8),
    city_pop INTEGER,
    job TEXT,
    dob DATE,
    acct_num VARCHAR(20),
    profile VARCHAR(50),
    updated_at TIMESTAMP
);

-- 2. Cards
CREATE TABLE cards (
    card_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES customers(customer_id),
    card_num VARCHAR(20),
    card_brand VARCHAR(50),
    card_type VARCHAR(20), -- credit, debit, prepaid
    credit_limit DECIMAL(15, 2),
    exp_date DATE,
    cvv VARCHAR(4),
    cardholder_name TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP
);

-- 3. Merchants
CREATE TABLE merchants (
    merchant_id SERIAL PRIMARY KEY,
    merchant_name TEXT,
    category VARCHAR(50),
    updated_at TIMESTAMP
);

-- 4. Transactions
CREATE TABLE transactions (
    trans_num VARCHAR(50) PRIMARY KEY,
    trans_date DATE,
    trans_time TIME,
    unix_time BIGINT,
    category VARCHAR(50),
    amt DECIMAL(15, 2),
    is_fraud SMALLINT, -- 0 or 1
    merchant_id INTEGER REFERENCES merchants(merchant_id),
    merchant_name TEXT,
    merch_lat DECIMAL(10, 8),
    merch_long DECIMAL(11, 8),
    customer_id INTEGER REFERENCES customers(customer_id),
    card_id INTEGER REFERENCES cards(card_id),
    trans_type VARCHAR(20) -- purchase, refund
);


CREATE INDEX idx_trans_customer ON transactions(customer_id);
CREATE INDEX idx_trans_date ON transactions(trans_date);
CREATE INDEX idx_cards_customer ON cards(customer_id);
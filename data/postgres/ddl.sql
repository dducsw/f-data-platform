-- Finance Transaction OLTP Database

create database finance_db;

\c finance_db;



drop table if exists users;
drop table if exists cards;
drop table if exists transactions;

-- Table: users
create table users (
    id                  integer         primary key,
    current_age         integer         not null,
    retirement_age      integer         ,
    birth_year          integer         ,
    birth_month         integer         ,
    gender              varchar(50)     check(gender in ('Male', 'Female')),
    address             varchar(255)    ,
    latitude            decimal(9,6)    ,
    longitude           decimal(9,6)    ,
    per_capita_income   varchar(50)     ,
    yearly_income       varchar(50)     ,
    total_debt          varchar(50)     ,
    credit_score        integer         ,
    num_credit_cards    integer         
);

-- Table: cards
create table cards (
    id                  integer         primary key,
    client_id           integer         not null references users(id),
    card_brand          varchar(50)     ,
    card_type           varchar(50)     ,
    card_number         varchar(50)     ,
    expires             varchar(10)     ,
    CVV                 varchar(10)     ,
    has_chip            varchar(5)     ,
    num_cards_issued    integer         ,
    credit_limit        varchar(50)     ,
    acct_open_date      varchar(10)     ,
    year_pin_last_changed integer       ,
    card_on_dark_web    varchar(5)      
);

-- Table: transactions
create table transactions (
    id                  integer         primary key,
    date_time           timestamp       not null,
    date                date            not null,
    client_id           integer         not null references users(id),
    card_id             integer         not null references cards(id),
    amount              varchar(50)     ,
    use_chip            varchar(50)     ,
    merchant_id         integer         ,
    merchant_city       varchar(50)     ,
    merchant_state      varchar(50)     ,
    zip                 varchar(50)     ,
    mcc                 integer         ,
    errors              varchar(255)    
);




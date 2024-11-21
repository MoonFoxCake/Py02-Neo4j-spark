-- creates database For postgres
CREATE DATABASE summaries;

\c summaries


CREATE TABLE Total_Spent_Per_Customer (
    customer_id VARCHAR PRIMARY KEY,
    Client_name VARCHAR,
    gender VARCHAR,
    address_location VARCHAR,
    job_title VARCHAR,
    total_spent NUMERIC
);

CREATE TABLE Product_Purchase_Count (
    product_id VARCHAR PRIMARY KEY, 
    size_of_product VARCHAR,
    product_line VARCHAR,
    Product_class VARCHAR,
    Manufacturer VARCHAR,   
    purchase_count INT
);

CREATE TABLE Average_Spend_Per_Customer (
    customer_id VARCHAR PRIMARY KEY,
    Client_name VARCHAR,
    gender VARCHAR,
    address_location VARCHAR,
    job_title VARCHAR,
    total_spent NUMERIC,
    transaction_count INT,
    average_spent NUMERIC
);

CREATE TABLE Transaction_Count_Per_Customer (
    customer_id VARCHAR PRIMARY KEY,
    Client_name VARCHAR,
    gender VARCHAR,
    address_location VARCHAR,
    job_title VARCHAR,
    total_spent NUMERIC,
    transaction_count INT
);

-- creates database For postgres
CREATE DATABASE Summaries;

\c Summaries


CREATE TABLE Total_Spent_Per_Customer (
    customer_id VARCHAR PRIMARY KEY,
    Client_name NVARCHAR,
    gender NVARCHAR,
    address_location NVARCHAR,
    job_title NVARCHAR,
    total_spent NUMERIC
);

CREATE TABLE Product_Purchase_Count (
    product_id VARCHAR PRIMARY KEY, 
    size_of_product NVARCHAR,
    product_line NVARCHAR,
    Product_id NVARCHAR,
    Product_class NVARCHAR,
    Manufacturer NVARCHAR,   
    purchase_count INT
);

CREATE TABLE Average_Spend_Per_Customer (
    customer_id VARCHAR PRIMARY KEY,
    Client_name NVARCHAR,
    gender NVARCHAR,
    address_location NVARCHAR,
    job_title NVARCHAR,
    total_spent NUMERIC,
    transaction_count INT,
    average_spent NUMERIC
);

CREATE TABLE Transaction_Count_Per_Customer (
    customer_id VARCHAR PRIMARY KEY,
    Client_name NVARCHAR,
    gender NVARCHAR,
    address_location NVARCHAR,
    job_title NVARCHAR,
    total_spent NUMERIC,
    transaction_count INT,
);

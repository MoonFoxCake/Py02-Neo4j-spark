import csv
import time
import logging
from neo4j import GraphDatabase, basic_auth, exceptions

driver = None

neo4j_uri = "neo4j://neo4j"
neo4j_user = "neo4j"
neo4j_password = "password"

for _ in range(99):
    try:
        print("Intentando conexión con Neo4j.", flush=True)
        driver = GraphDatabase.driver(neo4j_uri, auth=basic_auth(neo4j_user, neo4j_password))
        with driver.session() as session:
            session.run("RETURN 1")
        print("Conexión completada con Neo4j.", flush=True)
        break
    except exceptions.ServiceUnavailable:
        print("Neo4j no está listo. Reintentando en 5 segundos.", flush=True)
        time.sleep(5)

print("Procesando información de customerdemographic.csv.", flush=True)

data = []
with open("/app/csv/customerdemographic.csv", newline='') as csvfile:
    customerReader = csv.reader(csvfile)
    for row in customerReader:
        data.append({
            "customer_id": row[0],
            "name": row[1],
            "gender": row[2],
            "DOB": row[4],
            "age": row[5],
            "job_title": row[6],
            "job_industry_cat": row[7],
            "owns_car": row[10],
            "tenure": row[11]
        })

bloqueInstr = """
UNWIND $data AS row
MERGE (u:User {customer_id: row.customer_id})
ON CREATE SET 
    u.name = row.name,
    u.gender = row.gender,
    u.DOB = row.DOB,
    u.age = row.age,
    u.job_title = row.job_title,
    u.job_industry_cat = row.job_industry_cat,
    u.owns_car = row.owns_car,
    u.tenure = row.tenure
ON MATCH SET
    u.name = row.name,
    u.gender = row.gender,
    u.DOB = row.DOB,
    u.age = row.age,
    u.job_title = row.job_title,
    u.job_industry_cat = row.job_industry_cat,
    u.owns_car = row.owns_car,
    u.tenure = row.tenure
"""

with driver.session() as session:
    session.run(bloqueInstr, data=data)

print("Procesando información de customeraddress.csv.", flush=True)

data = []
with open("/app/csv/customeraddress.csv", newline='') as csvfile:
    customerReader = csv.reader(csvfile)
    for row in customerReader:
        data.append({
            "customer_id": row[0],
            "address": row[1],
            "postcode": row[2],
            "state": row[3],
            "country": row[4],
            "property_valuation": row[5]
        })

bloqueInstr = """
UNWIND $data AS row
MATCH (u:User {customer_id: row.customer_id})
SET u.address = row.address,
    u.postcode = row.postcode,
    u.state = row.state,
    u.country = row.country,
    u.property_valuation = row.property_valuation
"""

with driver.session() as session:
    session.run(bloqueInstr, data=data)

print("Procesando información de transactions.csv.", flush=True)

with open("/app/csv/transactions.csv", newline='') as csvfile:
    transactionsReader = csv.reader(csvfile)
    for row in transactionsReader:
        with driver.session() as session:
            session.run(
                """
                MERGE (p:Product {product_id: $product_id})
                ON CREATE SET 
                    p.product_brand = $product_brand,
                    p.product_line = $product_line,
                    p.product_class = $product_class,
                    p.product_size = $product_size
                """,
                product_id=row[1],
                product_brand=row[6],
                product_line=row[7],
                product_class=row[8],
                product_size=row[9]
            )

transactions = []
with open("/app/csv/transactions.csv", newline='') as csvfile:
    transactionsReader = csv.reader(csvfile)
    for row in transactionsReader:
        transactions.append({
            "customer_id": row[2],
            "product_id": row[1],
            "transaction_date": row[3],
            "transaction_amount": row[11]
        })

giant_cypher_block = """
UNWIND $transactions AS txn
MATCH (u:User {customer_id: txn.customer_id})
MATCH (p:Product {product_id: txn.product_id})
CREATE (u)-[r:PURCHASED]->(p)
SET 
    r.transaction_date = txn.transaction_date,
    r.transaction_amount = txn.transaction_amount
"""

with driver.session() as session:
    session.run(giant_cypher_block, transactions=transactions)

print("Final de procesamiento.", flush=True)
import csv
import time
import logging
from neo4j import GraphDatabase, basic_auth

driver = None

try:
    neo4j_uri = "neo4j://neo4j"
    neo4j_user = "neo4j"
    neo4j_password = "password"
    driver = GraphDatabase.driver(neo4j_uri, auth=basic_auth(neo4j_user, neo4j_password))
    time.sleep(60)
    print("""
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣤⣀⠹⢶⣤⠴⣷⣦⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠀⣀⠀⠀⣀⣶⣤⣄⣠⣶⣿⠏⢠⡄⣿⣾⣿⣿⣧⣠⡆⠀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣄⣀⣼⣿⣿⢿⣿⣿⣿⣿⣿⣿⣿⣷⣷⣾⣿⣿⣿⣿⣿⣿⣿⣿⣶⣿⣿⡶⢀⡆⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣾⡇⠀⣠⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⢀⡀⠀⠰⣴⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡏⢉⣿⢀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠈⣿⣶⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⣣⣿⣿⡀⠀⠠⢀⣀
⠀⠀⠀⠀⠀⢀⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⠶⣿⡏⠀
⠀⠀⠀⣠⣴⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠟⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣏⣀⣹⠀⠀
⠀⠀⣼⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡿⠿⠿⠟⠛⠛⠁⠀⠀⠈⠉⠛⠿⠿⣿⣿⣿⣿⣿⣿⣿⡇⠀⠀
⠀⣰⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡍⠛⠛⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠻⣿⣿⣿⣿⣧⠀⠀
⢠⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⢟⣿⣿⣿⠀⠀
⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡟⢿⡄⢦⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⣌⣿⣿⡇⠀
⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⡀⣻⣷⠈⢿⡄⠳⠆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⣿⣿⠀
⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣾⢿⡀⠀⠀⠀⠀⠀⠀⣀⣤⣤⣤⣤⣤⣤⣀⠀⠀⠀⠀⠀⠀⠀⢀⣸⣿⠀
⠘⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠄⠉⠉⠀⠀⠀⠀⣠⣾⠿⠟⠛⠉⠉⠉⠉⠙⠷⠄⠀⠀⠀⠀⣾⣿⣿⡟⠀
⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠁⠀⠀⠀⠀⠀⠀⡤⠛⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣲⠀⠀⠀⢀⣿⠁⠙⡇⠀
⠀⢸⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠃⠀⠀⠀⠀⠀⠀⠈⠀⠀⠀⣀⣴⡶⠶⣶⣤⣀⠀⣤⠏⠀⢠⣶⠟⢉⣀⣠⡇⠀
⠀⠸⢿⣿⣿⣿⢏⣰⣶⣤⣙⢿⣿⣿⣿⣿⣿⣿⣿⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠠⠾⠛⢁⡠⢴⣿⡿⠿⡼⠋⠀⠀⢸⣣⡴⠛⢹⣿⡇⠀
⠀⠀⢸⣿⣿⣿⡘⠿⠁⠘⠻⢷⠻⣿⣿⣿⣿⡏⠹⣧⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠰⣤⣤⣉⣴⣾⠟⠀⠀⠀⠀⠀⠳⣤⣑⣙⣿⡇⠀
⠀⠀⠘⣿⣿⣿⣧⠀⠀⣴⣶⣿⣷⡿⠀⠹⣿⣷⣿⡟⡆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠐⠒⠶⠿⠒⠁⠀⠀⠀⠀⠀⠀⠀⢤⣉⣿⣿⡅⠀
⠀⠀⠀⠈⢿⣿⣿⡄⠀⠘⡿⠋⣿⠀⠀⠀⣿⠿⣿⣧⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢨⡈⠉⢸⡇⠀
⠀⠀⠀⠀⠘⣿⣿⣿⡀⠀⠀⠀⠹⡄⠀⠀⣿⣧⣿⣷⡃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡀⠀⠀⠀⠀⠀⠀⠀⠇⠀⠘⣇⠀
⠀⠀⠀⠀⠀⢻⣿⣿⣿⣦⡀⠀⠀⠀⡇⠀⢾⣿⣿⣼⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠁⡀⠀⣀⣀⣀⣀⡀⣸⠀⢸⡍⠀
⠀⠀⠀⠀⠀⠘⣿⣿⣿⣿⣿⢶⣤⣴⡇⠀⠙⠛⠛⠻⠿⠆⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠆⠀⠀⠻⢿⣿⣿⣛⣯⡍⠁⢀⣾⠃⠀
⠀⠀⠀⠀⠀⠀⢻⣿⣿⣿⡟⠁⠈⣿⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠐⠀⠀⡠⠀⠀⠀⠀⠀⠀⠀⠈⡙⠻⣧⡀⣿⠋⠀⠀
⠀⠀⠀⠀⠀⠀⠘⣿⣿⣿⠁⠀⠐⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠠⠚⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣀⣻⡇⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠈⠈⣿⡶⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡶⠦⣀⡴⠂⣀⣠⣶⣾⣿⣿⣿⣿⡿⠃⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣿⢿⣶⣶⣾⣿⣿⣿⣿⣿⣿⣿⠛⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⢸⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢲⡌⠙⢿⣿⣿⣿⣿⣽⣇⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⢀⣟⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠲⣄⠈⠛⠻⢦⡿⢟⡿⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⡼⠇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⠷⠀⣀⣀⣠⠾⠁⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⢸⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠀⠀⠀⠠⡄⠀⣹⡟⢖⡶⠃⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠈⢧⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⣆⠀⠀⠀⢻⠰⠞⢃⣼⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠈⠳⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠀⠰⡆⠸⢰⣠⡾⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠘⢦⡐⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠙⠶⣜⣛⣛⣣⡼⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠙⠲⣤⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣀⣀⣀⠀⢐⢠⡿⠋⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⡉⠑⠒⠤⠤⡀⠀⠀⠀⠤⢀⠀⠀⠉⠀⣀⣤⠴⠚⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
        """)
except:
    print("Error en la conexión de Neo4J.")

with open("/app/csv/customerdemographic.csv", newline='') as csvfile:
    customerReader = csv.reader(csvfile)
    for row in customerReader:
        with driver.session() as session:
            session.run(
                """
                MERGE (u:User {customer_id: $customer_id, 
                               name: $name, 
                               gender: $gender, 
                               DOB: $DOB, 
                               age: $age,
                               job_title: $job_title,
                               job_industry_cat: $job_industry_cat,
                               owns_car: $owns_car,
                               tenure: $tenure })
                ON CREATE SET u.customer_id = $customer_id, 
                              u.name = $name, 
                              u.gender = $gender, 
                              u.DOB = $DOB, 
                              u.age = $age,
                              u.job_title = $job_title,
                              u.job_industry_cat = $job_industry_cat,
                              u.owns_car = $owns_car,
                              u.tenure = $tenure
                """,
                customer_id=row[0],
                name=row[1],
                gender=row[2],
                DOB=row[4],
                age=row[5],
                job_title=row[6],
                job_industry_cat=row[7],
                owns_car=row[10],
                tenure=row[11]
            )

with open("/app/csv/customeraddress.csv", newline='') as csvfile:
    customerReader = csv.reader(csvfile)
    for row in customerReader:
        with driver.session() as session:
            session.run(
                """
                MATCH (u:User {customer_id: $customer_id})
                SET u.address = $address,
                    u.postcode = $postcode,
                    u.state = $state,
                    u.country = $country,
                    u.property_valuation = $property_valuation
                """,
                customer_id=row[0],
                address=row[1],
                postcode=row[2],
                state=row[3],
                country=row[4],
                property_valuation=row[5]
            )

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

with open("/app/csv/transactions.csv", newline='') as csvfile:
    transactionsReader = csv.reader(csvfile)
    next(transactionsReader)  # Saltar encabezado si lo tiene
    for row in transactionsReader:
        with driver.session() as session:
            session.run(
                """
                MATCH (u:User {customer_id: $customer_id})
                MATCH (p:Product {product_id: $product_id})
                CREATE (u)-[r:PURCHASED]->(p)
                SET 
                    r.transaction_date = $transaction_date,
                    r.transaction_amount = $transaction_amount
                """,
                customer_id=row[2],
                product_id=row[1],
                transaction_date=row[3],
                transaction_amount=row[11]
            )
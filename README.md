# World-Port-Index-Data-Analysis
This project involves the migration of World Port data from an Access database to a relational database management system (PostgreSQL).

## Table of Contents
- [Objective](#objective)
- [Instructions](#instructions)
- [Requirements](#requirements)
- [Project Structure](#project-structure )
- [Conclusion](#Conclusion)
- [Contribution](#contribution)

## Objective
The primary goal of this project is to establish an Extract Load (EL) pipeline, utilizing Python, for the efficient transfer of data from the World Port Index contained within an Access database to a PostgreSQL database. Furthermore, the project also focuses on formulating and executing specific SQL queries pertinent to the dataset.

## Instructions
The following instructions should be adhered to for the purpose of achieving the project objectives
1. **Extract Load (EL) Pipeline**: Develop an Extract Load (EL) pipeline using Python to facilitate the smooth transfer of the World Port Index data from an Access database to a PostgreSQL database.
2. **Answer SQL Queries**: Address the following SQL queries using the data migrated into the PostgreSQL.
    1. The 5 nearest ports to Singapore's JURONG ISLAND port.
    2. Country with largest number of ports with a cargo_wharf.
    3. The port with the nearest access to provisions, water, fuel-oil and diesel.

## Requirements
- Python 3.x
- PostgreSQL database

## Project Structure  
`world_port.py`: The Python script in this project is designed with multiple functions, each serving a distinct purpose in the data processing pipeline. These functions include downloading the World Port Index data, establishing a connection to the Access database, converting the data into a Pandas DataFrame, exporting the data to a CSV file, and loading the data into the PostgreSQL database. Additionally, the script contains functions dedicated to addressing specific analytical questions as defined in the project requirements.
`util.py`: This utility script features a function named get_database_conn that is responsible for establishing a database connection. It utilizes credentials sourced from a .env file to securely manage the connection details.

## Conclusion
To summarize, this project entailed acquiring the World Port Index dataset from an Access database. The retrieval process was facilitated through the use of the pyodbc and pandas libraries to establish a connection with the Access database. Following this, the dataset was successfully migrated to a PostgreSQL database, a task accomplished using the psycopg2 and SQLAlchemy libraries.

## Contribution
Contributions to further enhance and expand this Extract Load (EL) pipeline is highly welcomed. Your involvement can take various forms, such as submitting pull requests, report issues, or offering valuable feedback. 

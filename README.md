## Football Database & Visualisation

## Project Aims
The aim of this project was to create a Postgres database of football data from Understats API which can be used for data visualisations going forward.


## Data Collection & Cleaning
Data was collected using Understat's API. Understat provides detailed football statistics and data. You can find more information and the documentation for the Understat API [here](https://understat.com/).
This project used the Shots and Match method provided by the API. Using pythons pandas package a minimal amount of data cleaning was carried out to ensure the data was clean and in the desired format before loading the data into a database.

## Database Management
### Table Creation
Using Python's SQLAlchemy package, a Postgres database was created with two tables within the database schema: one for the shots data and one for the match data. Each table has primary and foreign keys determined as constraints. All column types are specified during table creation. For more information on SQLAlchemy, you can refer to the [SQLAlchemy documentation](https://docs.sqlalchemy.org/).

### View Creation
A SQL script inclusivie of CTE's and Window Functions was then used to create a view of league table at each game week. For this the psycopg2 python package was used to interact with the database. 

For more information on psycopg2, you can refer to the [psycopg2 documentation](https://www.psycopg.org/docs/).

### Dashboard SQL Query
Using CTE's and aggregation functions in Postgresql a dataset was then crafted to satisfy the needs of a dashboard that was created in Tableau. 

## Dashboard Output
[Attack vs Defence: Shot Dynamics Across the Premier League Dashoard](https://public.tableau.com/views/AttackvsDefenceShotDynamicsAcrossthePremierLeague_17365252192810/AttackvsDefenceDashboard?:language=en-GB&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link) 

This dashboard was created to show on a singluar pitch map for each team where they take the most shots on opposition goals and where they concede the most shots on their own goal when defending. 

## Summary
This project involves creating a PostgreSQL database using football data from the Understat API, which is then used for data visualizations. Data collection and cleaning were performed using Python's pandas package. The database was managed with SQLAlchemy, and a view of the league table was created using SQL scripts with CTEs and Window Functions via psycopg2. The final output is a Tableau dashboard that visualizes shot dynamics across the Premier League, showing where teams take and concede the most shots.


## Requirements

To run this project, you will need the following Python packages, as specified in the `requirements.txt` file:

```
pandas==1.3.3
SQLAlchemy==1.4.22
psycopg2==2.9.1
requests==2.26.0
```

You can install these packages using pip:

```sh
pip install -r requirements.txt
```
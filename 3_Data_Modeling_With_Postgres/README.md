

# Introduction
> A startup called Sparkify collects users data that describe songs and users activities on their app. The analytics team tries to
> understand what songs their usage listen too. Currently, the data is collected in form of json files that 
> contain logs on user activity on the app as well as data about songs on their app. This project aims to organize data in a database that allows to
> analyze user activities and draw conclusions about it. In order to achieve more user-friendly interface and access to data,
> the database needs to be designed.


# Database schema design and ETL pipeline.
In order to achieve analytics goals, we decided to use Postgres database that is open source and allows 
to do aggregations and joins. Thus, this type of database is a perfect match for our purpose. The project directory contains 
following files:
    1. create_tables.py
    2. sql_queries.py
    3. etl.py
    4. test.ipynb

The create_tables.py file creates database and then it carries out instructions for creating and dropping tables. The sql_queries.py describes
SQL queries that we run in the create_tables.py file. Once the database is created, we can run etl.py file to populate our empty
tables. The test.ipynb allows to carry out manual tests to see if all previous operations were correct.




# Usage example
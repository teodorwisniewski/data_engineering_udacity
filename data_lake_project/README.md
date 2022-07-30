# Project: Data Lake




## Introduction

The project aims at organizing a large number of JSON files in order to allow the analytics team to draw conclusions
about the users' usage of the service "Sparkify." Two sets of JSON files can be distinguished. The first set of the JSON files contains information
about songs, such as artist name, title, duration of the song, and so on. The second dataset describes log sessions 
of Sparkify users. In the logs, we can find data about the usage of the services and which songs have been played for 
how long and how often. As the data is unorganized and it is difficult to draw valuable insights for the analytics team.
Thus, we decided to use the dimensional modeling to create a star schema for our data.


## General outline
The ETL process can be divised into the following step.
1. Loading data from s3 bucket
2. Transforming data data
3. Uploading data into the s3 bucket.

## The star schema of the database

The star schema contains the following tables:

* songplays - stands for the fact table containing the information about song plays.
* users - stands for the dimension table containing the information about song users of the app
* songs - stands for the dimension table containing the information about songs found in the app
* artist - stands for the dimension table containing the information about artits found in the app  
* time - stands for the dimension table containing the information about timestamps of records in the songsplay

## Example of use
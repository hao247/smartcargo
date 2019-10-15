# SmartCargo
This is a project I completed during the Insight Data Engineering program. Please visit dataprocessing.site to play with it (or watch it here).

Due to the growing need of fast and efficient shipping, the estimation of vessel arrival time becomes an essential demand for importers and ship owners. A more accurate arrival time avoids extra hours of charges from ports and helps companies schedule following road transport.

This project aims to provide a tool for people who concern about vessel logistics by using 934 GB of public AIS ship tracking data from 2015 to 2017. The pipeline performs data sessionization and groups ship location time series into port-to-port trips. The trip duration as well as port traffic will be estimated base on departure and desination ports from user inputs.

## Pipeline

Historical ship tracking data are ingested from S3 bucket into Spark, where timeseries data of each vessel will be sessionized based the vessel speed. The trip information such as starting/ending port and trip duration will be extracted and stored in a seperate table in database for front-end queries.

## Data Sources

1. AIS ship tracking data from 2015 to 2017 (934 GB in total).

2. US major ports data including 150 major ports with their names, locations, imports and outports.

## Environmental Setup

Install and configure AWS

### Cluster structure

### PostgreSQL setup

### configurations

## Running SmartCargo

### Schedule the Batch Processing

### Dash

### Testing


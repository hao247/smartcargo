# SmartCargo
> ***vessel tracks help schedule voyage logistics***

This is a project I completed during the Insight Data Engineering program. Please visit [dataprocessing.site](http://dataprocessing.site) to play the app or watch the demo [here](https://youtu.be/gaZ753xiWlI).

---

Due to the growing need of fast and efficient shipping, the estimation of vessel voyage becomes an essential demand for importers and ship owners. A more accurate arrival time avoids extra hport fees and helps companies schedule following road transport.

Base on analyzing historical ship trips from port to port, this project aims to provide a tool for people to better schedule voyage logistics. Once the starting and ending ports are specified, the app will provide statistics of trip duration and port traffic information on a dashboard so that people can perform better estimation of ship arrival and docking time.


## Data Pipeline
![alt text](https://github.com/hao247/smartcargo/blob/master/img/Data_pipeline.png "SmartCargo Pipeline")

Historical ship tracking data are ingested from S3 data bucket into Spark, where time series data of each vessel will be sessionized based on its speed. If the speed is below 0.1 knot for over 3 hours, the timeseries will be splitted by the speed gap. The trip information such as starting/ending port and trip duration will be extracted and stored in the trips table in database for front-end queries. The process is automated by using Airflow scheduler.

## Data Sources
  1. [AIS ship tracking data](https://marinecadastre.gov/ais/) from 2015 to 2017 (934 GB in total). This is the main dataset for trip sessionization and ship-type extraction.

  2. [US major ports data](https://catalog.data.gov/dataset/major-ports-national) including 150 major ports with their names, locations, imports and outports. This will be used for labeling starting and ending ports of trips.

## Repo directory structure

The directory layout is shown below:
```bash
.
├──airflow
│   ├── batch_scheduler.py  # Scheduler for batch processor
│   └── run_scheduler.sh    # Run scheduler
├── batch_processor
│   ├── batch_processor.py  # Data processor by using pyspark
│   └── run_batch.py        # Run batch processsor
├── config
│   ├── credentials.yaml          # Authentication info
│   ├── credentials.yaml.example
│   ├── s3_config.yaml            # List of importing files in s3
│   └── schema.yaml               # Schema stored in psql
├── dash
│   ├── app.py    # Web application
│   └── plots.py  # Plot methods used in WebUI
├── img
│   └── Data-pipeline.png
├── LICENSE 
├── README.md
├── requirements.txt
├── spark-run.sh  # Start spark pipeline
├── test
│   ├── benchmark_label_functions.py  # benchmark different labeling methods
│   └── unit_tests.py                 # unittest of methods in tools.py and label_bucketizer.py
└── tools
    ├── label_bucketizer.py  # port identification method by using bucketizer
    ├── label_pandas.py      # port identification method by using pandas cut
    ├── label_sql.py         # port identification method by using SQL commands
    ├── postgres.py          # methods for PostgreSQL IO
    └── tools.py             # methods for trip identification
```

## Environmental Setup

#### cluster setup
* Install [Pegasus](https://github.com/InsightDataScience/pegasus) on your local machine.
* For AWS VPC, 6 m4.large AWS EC2 instances are needed to run project, 4 nodes for spark cluster, 1 node for database and 1 node for dash application.
* In order to set up the cluster, `master.yml` and `workers.yml` files in `<PEGASUS_HOME>/examples/spark/` folder are needed. After the configuration files are ready, install the spark cluster by running 
  ```bash
  <PEGASUS_HOME>/examples/spark/spark_hadoop.sh
  ``` 
  You can install other programs on the cluster by running
  ```bash
  peg install <cluster-name><technology>
  ```
* Install [AWS CLI](https://aws.amazon.com/cli/) and add your local IP to your AWS VPC inbound rules.
* Once the cluster is ready, this repository should be cloned to the cluster with required python packages insalled.

#### PostgreSQL/TimescaleDB setup
* The PostgreSQL database is installed on a seperate database instance. To install it, please run
  ```bash
  sudo apt install postgresql-11
  ```
* Download the [jar file](https://jdbc.postgresql.org/download/postgresql-42.2.6.jar).
* Create user, database, password
* Change peer to md5 for the user and add these lines to `/etc/postgresql/11/main/pg_hba.conf`
  ```
  host all all 0.0.0.0/0 md5
  host all all ::/0 md5
  ```
* Grant permissions in psql:
  ```sql
  alter user <username> with encrypted password <password>;
  grant all privileges on database <dbname> to <username>;
  ```
* Add TimescaleDB's third party repository and install TimescaleDB:
  ```bash  
  sudo add-apt-repository ppa:timescale/timescaledb-ppa
  sudo apt-get update
  sudo apt install timescaledb-postgresql-11
  ```
* Configure TimescaleDB by running
  ```bash
  timescaledb-tune
  ```
* Restart PostgreSQl service:
  ```bash
  sudo service postgresql restart
  ```
  
#### Airflow setup
* Install and initialize Airflow:
  ```bash
  pip3 install apache-airflow
  airflow initdb
  ```
* Launch webserver and scheduler:
  ```bash
  nohup airflow webserver -p 8082 &
  nohup airflwo scheduler &
  ```
  
## Running SmartCargo
#### Batch Processor
To add the batch processor job to the scheduler, run 
```bash
airflow/run_schedule.sh
```
on the master node. If there is a failure during the processing, an email will be sent to the email address specified in the credential file, and another job will be scheduled 5 minutes after the failure. The job can be monitored on the Airflow GUI at `http://<spark-cluster master-ip>:8082`.

#### Dash application
Run `dash/run_app.sh` to start the web server.

#### Testing
The folder `test/` contains unit tests for trip/port identification methods used in this project.

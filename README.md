# Pipeline Orchestration of Global Store Data - Airflow
  
<img width="808" alt="Screenshot 2021-05-09 at 10 16 22" src="https://user-images.githubusercontent.com/61795377/117565040-9e81bc80-b0af-11eb-94c6-e12ddae4ca0e.png">

[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)


Apache Airflow (or simply Airflow) is a platform to programmatically author, schedule, and monitor workflows.

When workflows are defined as code, they become more maintainable, versionable, testable, and collaborative.

Use Airflow to author workflows as directed acyclic graphs (DAGs) of tasks. The Airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

DAG — directed acyclic graph. How to run a workflow, visible in the dashboard on the web interface.  
Worker — one or more systems responsible for running the code in the workflow task queues.  
Webserver — displays UI for managing Airflow, manages user requests for running tasks, and receives updates from DAG runs via workers  
Scheduler — determines if a Task needs to be run and triggers work to be processed by a Worker  
Operator — a step of what actually gets run inside a DAG  
Task — an instantiated Operator created by the scheduler, a single unit of work  
Task Instance — stored state of a task.  

## Table of contents 
[1. Getting Started](#gettingstarted)  
<a name="gettingstarted"/>
[2. Architecture](#architecture)  
<a name="architecture"/>
[3. Tools and Technologies used](#toolsandtechnology)  
<a name="toolsandtechnology"/>
[4. Installation setup in Mac/Windows](#install)  
<a name="install"/>
[5. Startup Scripts](#startup)  
<a name="startup"/>
[6. Global Store source code](#sourcecode)  
<a name="sourcecode"/>
[7. Data Modelling Overview](#dbmodel)  
<a name="dbmodel"/>
[8. Running DAG](#dag)  
<a name="dag"/>
[9. Report Generation](#report)  
<a name="report"/>

## Getting Started

The purpose of this process is to take a production like source from a sales system and turn it into a data warehouse schema for reporting.  
Scales for multiple developers as the team grows. 
Schedules SQL transformation jobs.  
Handles the complexity of transformations and allows for easier maintenance. 
Reduces the time to market of producing new additional changes to the model and is sympathetic to the new data sources such as APIs, JSON or CDC from relational database sources.  
The value in delivering data warehouse pipelines is in the transforming source data using business rules into useful, clean, reliable data and spending a lot of time considering integration complexity

## Architecture

The Setup can work in any Local PC (Windows/Mac) - Tools used are Opensource & free to use. 

Architecture is quite simple – We have all our script written in python to load the CSV
file into SQLite DB . We have assumed to run this architecture in our Local machine
Below architecture diagram describes the simple usage. 


<img width="401" alt="Screenshot 2021-05-09 at 11 55 45" src="https://user-images.githubusercontent.com/61795377/117567716-9a5c9b80-b0bd-11eb-933b-bfb4802553b0.png">



## Tools and Technologies used

Apache Airflow  
Sqlite DB  
Python  
Sqlite DB browser  
Github Desktop  
Atom - Editor   
Pycharm   

## Installation setup in Mac/Windows

Step to install airflow on windows without Docker

Step 1: Control Panel | Programs and Features | Turn Windows features on or off

Enable : Windows Subsystem for Linux

Step 2: Install Ubuntu from windows store and restart system

Step 3: Install and update PIP

sudo apt-get install software-properties-common
sudo apt-add-repository universe
sudo apt-get update
sudo apt-get install python-pip

Step 4: Install airflow 

Note:
if above command doesn't work use below
python3 -m pip install apache-airflow

export SLUGIFY_USES_TEXT_UNIDECODE=yes
pip install apache-airflow

Step 5: Initialize DB 
airflow initdb

Step 6: Start airflow server
airflow webserver -p 8080

Step 7: URL is ready : http://localhost:8080/​

Step 8: Setup folder for DAG

create folder DAG in C: drive (C:\DAG)

Step 9: Add new DAG 

run airflow initdb

Follow these steps to install Airflow on Mac.  


Step 1: Open a Terminal window.  
Navigate to your Desktop in your Terminal. 

Step 2: We are going to create a working directory here but you can create anywhere else in your file system if you like.
cd Desktop.  

Step 3: Create a working directory here called airflow_learning.  
mkdir airflow_learning  

Step 4: Change to airflow-tutorial directory in your Terminal.  
cd airflow_learning  

Step 5: Create a virtual environment using coda (or any other tool). Install Python 3.7 in your virtual environment. I’m going to call the environment airflow-tutorial.  
conda create --name airflow_learning python=3.7  

Step 6: Activate the virtual environment.  
conda activate airflow_learning  

Step 7: Print the absolute path to your working directory by typing pwd.  
I get /Users/dineshmk/airflow_learning but you might get something different. Copy this path  

Step 8: Set the path as the AIRFLOW_HOME environment variable. Note that you have to do this every time you open a new Terminal window and wish to use the Airflow CLI. Alternatively, you can set a permanent environment variable in your bash_profile.  
export AIRFLOW_HOME=/Users/dineshmk/airflow_learning  
By default, airflow uses ~/airflow as it’s AIRFLOW_HOME directory. We can overwrite this by setting a different path. Airflow will initialize the airflow.cfg file here along with the logs folder. We’ll store our dags and plugins in this directory.  

Step 9: Install Airflow 1.10.10 + extras using pip  
pip install apache-airflow[gcp,statsd,sentry]==1.10.10  
If you’re using zsh like me then you need to put apache-airflow[gcp,statsd,sentry] in quotes as shown below.  
pip install 'apache-airflow[gcp,statsd,sentry]'==1.10.10  



## Startup Scripts

Setup Environment variable AIRFLOW_HOME to point to the directory created.  
Edit the airflow.cfg file to point the sql_alchemy_conn to ./db/airflow.db. 
Same for the logs

## Global Store source code

The Source code start from DAG folder - Dependencies are set here 
Which triggers the following  

DB Creation Setup : sqlite_db_script.py  
Import from Source (CSV,XLSX) to Staging table : sqlite_db_csv_import_returns.py,sqlite_db_xlsx_import_orders.py & sqlite_db_xlsx_import_people.py  
Dimension Load : sqlite_db_customer_dim.py,sqlite_db_date_dim,sqlite_db_product_dim,sqlite_db_people_dim,sqlite_db_market_dim.py  
Fact Sales : sqlite_db_fact.py,sqlite_db_report.py  
Reporting Load : sqlite_db_report.py  


## Data Modelling Overview

High Level View.  

<img width="616" alt="Screenshot 2021-05-09 at 11 35 37" src="https://user-images.githubusercontent.com/61795377/117567098-b0b52800-b0ba-11eb-9bb6-17c0d257c24b.png">

Data Model.  

<img width="1125" alt="Screenshot 2021-05-09 at 11 31 20" src="https://user-images.githubusercontent.com/61795377/117567000-31275900-b0ba-11eb-9ff0-1f1678dfaa25.png">


## Running DAG

Click on Trigger DAG in the Airflow webserver  & click on the Tree view to see the Success and failure process.  

<img width="1306" alt="Screenshot 2021-05-09 at 11 33 26" src="https://user-images.githubusercontent.com/61795377/117567054-7186d700-b0ba-11eb-818b-2c1b98b964ae.png">

## Report Generation

Required Reporting needs scripts are triggerred at the last can be querried from Sqlite DB browser.

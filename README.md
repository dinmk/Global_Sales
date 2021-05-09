# Pipeline Orchestration of Global Store Data - Airflow
  
<img width="808" alt="Screenshot 2021-05-09 at 10 16 22" src="https://user-images.githubusercontent.com/61795377/117565040-9e81bc80-b0af-11eb-94c6-e12ddae4ca0e.png">

[![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger)


Apache Airflow (or simply Airflow) is a platform to programmatically author, schedule, and monitor workflows.

When workflows are defined as code, they become more maintainable, versionable, testable, and collaborative.

Use Airflow to author workflows as directed acyclic graphs (DAGs) of tasks. The Airflow scheduler executes your tasks on an array of workers while following the specified dependencies. Rich command line utilities make performing complex surgeries on DAGs a snap. The rich user interface makes it easy to visualize pipelines running in production, monitor progress, and troubleshoot issues when needed.

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
[10. Conclusion](#conc)  
<a name="conc"/>

## Getting Started

The purpose of this process is to take a production like source from a sales system and turn it into a data warehouse schema for reporting.  
Scales for multiple developers as the team grows. 
Schedules SQL transformation jobs.  
Handles the complexity of transformations and allows for easier maintenance. 
Reduces the time to market of producing new additional changes to the model and is sympathetic to the new data sources such as APIs, JSON or CDC from relational database sources.  
The value in delivering data warehouse pipelines is in the transforming source data using business rules into useful, clean, reliable data and spending a lot of time considering integration complexity

## Architecture
## Tools and Technologies used
## Installation setup in Mac/Windows
## Startup Scripts
## Global Store source code
## Data Modelling Overview
## Running DAG
## Report Generation
## Conclusion

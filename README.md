# Migrating from Oozie to CDE with the oozie2cde API

![alt text](img/oozie2cde_slide.png)


## Project Summary

This tutorial demonstrates how you can leverage the oozie2cde API to programmatically translate and transfer your Oozie Workflows to your CDE Cluster.

The API is made of three modules and can help in your migration in three main areas:

1. Parse Oozie Workflows along with related files such as hive and oozie properties.
2. Convert Oozie Workflows into CDE API Payloads. 
3. Perform CDE API actions such as uploading files, creating jobs, etc. with Python Requests in order to execute the migration.


#### Supported Oozie Workflows

In its current version the tool supports four types of Oozie Actions and Airflow Operators:

* Spark -> CDEJobRunOperator
* Hive -> CDWJobRunOperator
* Email -> EmailOperator
* Shell -> BashOperator


#### Recommended Use

The notebook demonstrates an end to end example. It goes through the following motions:

1. Parsing a Hive Oozie Workflow with a properties file and three hive sql jobs
2. Parsing a Spark Oozie Workflow
3. Composing an Airflow DAG from the Spark Oozie Workflow
4. Creating a CDE Resource and uploading the Spark and Airflow DAG files to it
5. Creating corresponding Spark and Airflow CDE Jobs using the same CDE Resource files

_**It's important that you remember the following as you go along**_

* Once the Airflow DAG is complete, make sure you add the code to [declare individual task dependencies](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html#task-dependencies) at the bottom of the file. 
* Check all CDW Query syntax in the Airflow DAG file. The included parser methods may not always reformat the SQL syntax perfectly.
* Each Oozie Workflow and all related files should be located in an individual folder in order to parse them correctly with the Oozie Workflow module.


#### Prerequisites

This project requires access to a CDE Virtual Cluster and the Oozie Workflow files (workflow.xml and optionally any other files such as properties and hive sql). 
The CDE VC could be either in CDP Public or Private Cloud. 
Familiarity with Python, CLI’s and API’s in general, and Jupyter Notebooks is recommended. 


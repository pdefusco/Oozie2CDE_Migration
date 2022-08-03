import numpy as np
import pandas as pd
import os
from os.path import exists
import json
import sys
import re
import requests
from requests_toolbelt import MultipartEncoder
import xmltodict as xd
import pyparsing


class CdeJob:
    '''Class representing a CDE Job'''
    
    def __init__(self, workflow_xml_dict, cde_resource_name, dag_name=None):
        self.workflow_xml_dict = workflow_xml_dict
        self.cde_resource_name = cde_resource_name
        self.dag_name = dag_name
 
    
    def initialize_dag(self, dag_dir, dag_file_name):
        with open(dag_dir+"/"+dag_file_name, 'w') as f:
            f.write('# The new Airflow DAG')
    
    
    def dag_imports(self, dag_dir, dag_file_name):
        imports = """\nfrom dateutil import parser
    \nfrom datetime import datetime, timedelta
    \nimport pendulum
    \nfrom airflow import DAG
    \nfrom airflow.operators.email import EmailOperator
    \nfrom airflow.operators.python_operator import PythonOperator
    \nfrom cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
    \nfrom cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator\n\n"""
    
        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(imports)

    
    def dag_declaration(self, dag_owner, dag_dir, dag_file_name):
    
        declaration = """default_args = {{
        'owner': '{}',
    'retry_delay': timedelta(seconds=5),
    'depends_on_past': False,
    'start_date': pendulum.datetime(2016, 1, 1, tz="Europe/Amsterdam")
    }}

{} = DAG(
    'airflow-pipeline-demo',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    is_paused_upon_creation=False
    )\n\n""".format(dag_owner, self.dag_name)
    
        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(declaration)
    
    
    def parse_spark_oozie_action(self, a):

        if "spark" in a.keys():

            task_id = a["spark"]["name"]
            step_name = task_id+"_Step" 
            step_name = step_name.replace('-', '')
            spark_cde_job_name = task_id

            print("Extracted Job Name: {}".format(task_id))

            return task_id, step_name, spark_cde_job_name

        else:
            print("Error. This is not a Spark Oozie Action")
    
    
    def parse_hive_oozie_action(self, a):
    
        #Checking if this is a Hive Oozie Action
        if "hive" in a.keys():

            #CDE Operator Task ID
            task_id = a['@name']

            #CDW Operator Name
            step_name = task_id+"_Step"
            step_name = step_name.replace('-', '')

            #Parsing SQL from Hive file
            with open(a['hive']['script'], 'r') as f:
                hive_sql = f.read()
                cdw_query = hive_sql.replace("\n", "")

        return task_id, step_name, cdw_query
    
    
    def parse_email_oozie_action(self, a):
    
        if "email" in a.keys():

            #Task ID
            task_id = a['@name']

            #Operator Name
            step_name = task_id+"_Step"
            step_name = step_name.replace('-', '')

            #Extracting Email Fields

            action = a['email']

            if action.__contains__('to'):
                email_to = a['email']['to'] 
            if action.__contains__('cc'):
                email_cc = a['email']['cc']
            if action.__contains__('subject'):
                email_subject = a['email']['subject']
            if action.__contains__('body'):
                email_body = a['email']['body']

            return task_id, step_name, email_to, email_cc, email_subject, email_body
    
    
    def parse_shell_oozie_action(self, a):
    
        if "shell" in a.keys():

            #CDE Operator Task ID
            task_id = a['@name']

            #CDW Operator Name
            step_name = task_id+"_Step"
            step_name = step_name.replace('-', '')

            return task_id, step_name
    

    def append_cde_spark_operator(self, dag_dir, dag_file_name, task_id, step_name, spark_cde_job_name):
    
        spark_operator = """{} = CDEJobRunOperator(
        task_id='{}',
        dag={},
        job_name='{}'
        )\n\n""".format(step_name, task_id, self.dag_name, spark_cde_job_name)

        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(spark_operator)
    
    
    def append_cdw_operator(self, dag_dir, dag_file_name, task_id, step_name, cdw_query):    
    
        cdw_operator = '''cdw_query = """{}"""

{} = CDWOperator(
    task_id="{}",
    dag={},
    cli_conn_id="hive_conn",
    hql=cdw_query,
    schema='default',
    ### CDW related args ###
    use_proxy_user=False,
    query_isolation=True
)\n\n'''.format(cdw_query, step_name, task_id, self.dag_name)
    
        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(cdw_operator)
    
    
    def append_email_operator(self, dag_dir, dag_file_name, task_id, step_name, email_to, email_cc, email_subject, email_body):
    
        email_operator ='''
    {} = EmailOperator( 
    task_id="{}", 
    to="{}", 
    cc="{}",
    subject="{}", 
    html_content="{}", 
    dag={})
        '''.format(step_name, task_id, email_to, email_cc, email_subject, email_body, self.dag_name)

        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(email_operator)
    
    
    def append_bash_operator(self, dag_dir, dag_file_name, task_id, step_name):
    
        bash_operator = '''{} = BashOperator(
        task_id="{}",
        bash_command="echo \'here is the message'")'''.format(task_id, step_name)

        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(bash_operator)
    
    
    def append_python_operator(self, dag_dir, dag_file_name):
    
        print("Action not Found. Replacing Action with Airflow Python Operator Stub")

        task_id = "PythonOperator"
        step_name = "StepStub"

        python_operator = """\ndef my_func():\n\tpass\n 
        {} = PythonOperator(task_id='{}', python_callable=my_func)""".format(step_name, task_id)

        with open(dag_dir+"/"+dag_file_name, 'a') as f:
            f.write(python_operator)
    
    
    def oozie_to_cde_spark_payload(self, a):
        
        spark_cde_payload = { "name": "job_name", 
                "type": "spark", 
                "retentionPolicy": "keep_indefinitely", 
                "mounts": [ { "dirPrefix": "/", "resourceName": "resource_name" } ], 
                "spark": { "file": "file_name", 
                          "conf": { "spark.pyspark.python": "python3" }}, 
                "schedule": { "enabled": False}
               }
            
        if "spark" in a.keys():

            spark_cde_payload["name"] = a["spark"]["name"]
            spark_cde_payload["mounts"][0]["resourceName"] = self.cde_resource_name 
            spark_cde_payload["spark"]["file"] = a["spark"]["jar"].split("/")[-1]

            if len(a["spark"]["spark-opts"])>0:
                opts = a["spark"]["spark-opts"]
                spark_job_opts = dict(np.array_split(opts.split(" "), len(opts.split(" "))/2))

            if "--driver-cores" in spark_job_opts.keys():
                spark_cde_payload["spark"]["driverCores"] = int(spark_job_opts["--driver-cores"])

            if "--executor-cores" in spark_job_opts.keys():
                spark_cde_payload["spark"]["executorCores"] = int(spark_job_opts["--executor-cores"])

            if "--driver-memory" in spark_job_opts.keys():
                spark_cde_payload["spark"]["driverMemory"] = spark_job_opts["--driver-memory"]

            if "--executor-memory" in spark_job_opts.keys():
                spark_cde_payload["spark"]["executorMemory"] = spark_job_opts["--executor-memory"]

            if "--num-executors" in spark_job_opts.keys():
                spark_cde_payload["spark"]["numExecutors"] = int(spark_job_opts["--num-executors"])

            #if "class" in d["workflow-app"]["action"]["spark"].keys():
            #    cde_payload["spark"]["className"] = d["workflow-app"]["action"]["spark"]["class"]

        else:
            print("Error. This is not a Spark Oozie Action")

        print("Working on Spark CDE Job: {}".format(spark_cde_payload["name"]))
        print("Converted Spark Oozie Action into Spark CDE Payload")

        return spark_cde_payload

    
    def oozie_to_cde_airflow_payload(self, file_name, resource_name, cde_job_name):

    ## Variables: CDE file name, resource name and job name 
    
        airflow_cde_payload = {"type": "airflow", 
                               "airflow": {"dagFile": "file_name"}, #"my_dag.py"
                               "identity": {"disableRoleProxy": True},
                               "mounts": [{"dirPrefix": "/","resourceName": "resource_name"}],
                               "name": "cde_job_name",
                               "retentionPolicy": "keep_indefinitely"}
        
        airflow_cde_payload["airflow"]["dagFile"] = file_name
        airflow_cde_payload["mounts"][0]["resourceName"] = resource_name
        airflow_cde_payload["name"] = cde_job_name
        
        print("Working on Airflow CDE Job: {}".format(airflow_cde_payload["name"]))
        print("Converted DAG into Airflow CDE Payload")

        return airflow_cde_payload
    
    
    def parse_oozie_workflow(self, dag_dir, dag_file_name, workflow_xml_dict):

        spark_payloads = []
        
        if isinstance(workflow_xml_dict['workflow-app']['action'], dict):
            
            if 'hive' in workflow_xml_dict['workflow-app']['action'].keys():

                #Parsing Hive Oozie Action
                task_id, step_name, cdw_query = self.parse_hive_oozie_action(action)

                #Converting Hive Oozie Action to CDW Operator and Appending to CDE DAG
                self.append_cdw_operator(dag_dir, dag_file_name,task_id, step_name, cdw_query)

            elif 'spark' in workflow_xml_dict['workflow-app']['action'].keys():
        
                #Parsing Spark Oozie Action
                task_id, step_name, spark_cde_job_name = self.parse_spark_oozie_action(workflow_xml_dict['workflow-app']['action'])

                #Converting Spark Oozie Action to CDE Operator and Appending to CDE DAG
                self.append_cde_spark_operator(dag_dir, dag_file_name, task_id, step_name, spark_cde_job_name)
                
                #Create Spark Payload
                spark_cde_payload = self.oozie_to_cde_spark_payload(workflow_xml_dict['workflow-app']['action'])
                spark_payloads.append(spark_cde_payload)
                
            elif 'email' in workflow_xml_dict['workflow-app']['action'].keys():

                #Parsing Email Oozie Action
                task_id, step_name, email_to, email_cc, email_subject, email_body = self.parse_email_oozie_action(workflow_xml_dict['workflow-app']['action'])

                #Converting Email Oozie Action to CDE Airflow Email Operator
                self.append_email_operator(dag_dir, dag_file_name, task_id, step_name, email_to, email_cc, email_subject, email_body)

            elif 'shell' in workflow_xml_dict['workflow-app']['action'].keys():

                #Parsing Shell Oozie Action
                task_id, step_name = self.parse_shell_oozie_action(a)

                #Converting Shell Oozie Action to CDE Airflow Bash Operator
                self.append_bash_operator(dag_dir, dag_file_name, task_id, step_name)

            else:
                #Converting Unsupported Oozie Action to CDE Airflow Python Operator
                self.append_python_operator(dag_dir, dag_file_name)
            
        if isinstance(workflow_xml_dict['workflow-app']['action'], list):
            
            for action in workflow_xml_dict['workflow-app']['action']:

                print(action)

                if 'hive' in action.keys():

                    #Parsing Hive Oozie Action
                    task_id, step_name, cdw_query = self.parse_hive_oozie_action(action)

                    #Converting Hive Oozie Action to CDW Operator and Appending to CDE DAG
                    self.append_cdw_operator(dag_dir, dag_file_name,task_id, step_name, cdw_query)

                elif 'spark' in action.keys():

                    #Parsing Spark Oozie Action
                    task_id, step_name, spark_cde_job_name = self.parse_spark_oozie_action(action)

                    #Converting Spark Oozie Action to CDE Operator and Appending to CDE DAG
                    self.append_cde_spark_operator(dag_dir, dag_file_name, task_id, step_name, spark_cde_job_name)

                    #Create Spark Payload
                    spark_cde_payload = self.oozie_to_cde_spark_payload(action)
                    spark_payloads.append(spark_cde_payload)

                elif 'email' in action.keys():

                    #Parsing Email Oozie Action
                    task_id, step_name, email_to, email_cc, email_subject, email_body = self.parse_email_oozie_action(action)

                    #Converting Email Oozie Action to CDE Airflow Email Operator
                    self.append_email_operator(dag_dir, dag_file_name, task_id, step_name, email_to, email_cc, email_subject, email_body)

                elif 'shell' in action.keys():

                    #Parsing Shell Oozie Action
                    task_id, step_name = self.parse_shell_oozie_action(a)

                    #Converting Shell Oozie Action to CDE Airflow Bash Operator
                    self.append_bash_operator(dag_dir, dag_file_name, task_id, step_name)

                else:
                    #Converting Unsupported Oozie Action to CDE Airflow Python Operator
                    self.append_python_operator(dag_dir, dag_file_name)
        
        return spark_payloads
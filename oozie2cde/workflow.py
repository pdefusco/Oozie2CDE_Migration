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


class OozieWorkflow:
    '''Class representing the Oozie Workflow we want to convert
       The Oozie workflow is an xml file optionally accompanied 
       by other files such as properties, hive sql syntax, etc.
       Assumes the user has added all files to a dedicated local dir
       '''
    
    def __init__(self, project_path):
        self.project_path = project_path
    
    
    def ooziexml_to_dict(self):
        
        #"oozie_workflows/oozie_hive_workflow_with_properties"
    
        #Ensuring there is only one workflow xml file in the dir
        if len([file for file in os.listdir(self.project_path) if ".xml" in file]) == 1:

            #Parsing properties file
            for file in os.listdir(self.project_path):
                if ".xml" in file:
                    print("Oozie workflow file {} found".format(file))
                    with open(self.project_path+"/"+file,'rb') as f:
                        workflow_d = xd.parse(f)
                        return workflow_d

        elif len([file for file in os.listdir(self.project_path) if ".xml" in file]) == 0:

            print("No Oozie workflow file found.\n")
            print("If Oozie workflow file is expected, please ensure it is in the workflow directory.\n")
            print("If Oozie workflow file is not expected, please ignore this message.\n")

        else:

            print("Error. Only one Oozie workflow file per workflow directory expected.\n")
            print("Please remove the Oozie workflow file that is not associated with this project.\n")
    
    
    def parse_workflow_properties(self):
        
        #"oozie_workflows/oozie_hive_workflow_with_properties"
    
        #Ensuring there is only one properties file in the dir
        if len([file for file in os.listdir(self.project_path) if ".properties" in file]) == 1:

            #Parsing properties file
            for file in os.listdir(self.project_path):
                if ".properties" in file:
                    print("Properties file {} found".format(file))
                    with open(self.project_path+"/"+file) as f:
                        properties_file = f.read()
                        properties_dict = dict([tuple(i.split("=")) for i in properties_file.split("\n") if len(tuple(i.split("="))) > 1])
                        properties_dict = {x.replace(' ', ''): v.replace(' ','') for x, v in properties_dict.items()}

                        return properties_dict

        elif len([file for file in os.listdir(self.project_path) if ".properties" in file]) == 0:

            print("No properties file found.\n")
            print("If properties file is expected, please ensure it is in the workflow directory.\n")
            print("If properties file is not expected, please ignore this message.\n")

        else:

            print("Error. Only one properties file per workflow directory expected.\n")
            print("Please remove the properties file that is not associated with this workflow.\n")
    
    
    def workflow_properties_lookup(self, workflow_d, properties_dict):
        
        #Property value lookup 
        string = json.dumps(workflow_d)

        for k, v in properties_dict.items():
            string = string.replace(k, v)

        #Removing unwanted characters
        subbed = re.sub(r'"\${(.*?)}"', r'"\1"', string)
        workflow_d_props = json.loads(subbed)
        return workflow_d_props
    
    
    def query_properties_lookup(self, properties_dict):
        
        for file_name in os.listdir(self.project_path):
            if ".hive" in file_name:
                
                print("\nEditing Hive Query File: {}".format(file_name))
                
                with open(self.project_path+"/"+file_name, "r") as f:
                    hive_sql = f.read()

                print("\nThe input Hive query is: \n")
                print(hive_sql)

                for k, v in properties_dict.items():
                    hive_sql = hive_sql.replace(k, v)

                #Removing unwanted characters
                subbed = re.sub(r'\${(.*?)}', r'"\1"', hive_sql)

                print("\nThe output Hive query is: \n")
                print(subbed)
                
                subbed_list = subbed.split("\n")
          
                with open(self.project_path+"/"+file_name, "w") as f:
                    f.write("")
                    
                with open(self.project_path+"/"+file_name, "a") as f:
                    f.write(subbed)

    

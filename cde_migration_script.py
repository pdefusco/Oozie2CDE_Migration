import os
import json
import sys
import requests

os.environ["WORKLOAD_USER"] = sys.argv[0]
os.environ["WORKLOAD_PASSWORD"] = sys.argv[1]
os.environ["JOBS_API_URL"] = sys.argv[2]
file_names_list = sys.argv[3]
job_names_list = sys.argv[4]

# Set user token to interact with CDE Service remotely
def set_cde_token():
    rep = os.environ["JOBS_API_URL"].split("/")[2].split(".")[0]
    os.environ["GET_TOKEN_URL"] = os.environ["JOBS_API_URL"].replace(rep, "service").replace("dex/api/v1", "gateway/authtkn/knoxtoken/api/v1/token")
    token_json = !curl -u $WORKLOAD_USER:$WORKLOAD_PASSWORD $GET_TOKEN_URL
    os.environ["ACCESS_TOKEN"] = json.loads(token_json[5])["access_token"]
    return json.loads(token_json[5])["access_token"]

# Create CDE Resource to upload Spark CDE Job files
def create_cde_resource(tok, resource_name):

    {CDE_TOKEN} = tok

    url = os.environ["JOBS_API_URL"] + "/resources"
    myobj = {"name": str(resource_name)}
    data_to_send = json.dumps(myobj).encode("utf-8")

    headers = {
        'Authorization': f"Bearer {{CDE_TOKEN}}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    x = requests.post(url, data=data_to_send, headers=headers)
    print(x.status_code)

#Upload Spark CDE Job file to CDE Resource
def post_files(resource_name, file, tok):

    {CDE_TOKEN} = tok

    headers = {
        'Authorization': f"Bearer {{CDE_TOKEN}}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    files = {
        'file': open(file_path, 'rb'),}

    PUT = 'http://{jobs_api_url}/resources/{resource_name}/{file_name}.py'.format(jobs_api_url=os.environ["JOBS_API_URL"], resource_name=resource_name, file_name=file_name)

    x = requests.put(PUT, headers=headers, files=files)
    print(x.status_code)

#Create CDE Job
def post_cde_job(job_name, resource, file, tok):

    {CDE_TOKEN} = tok

    headers = {
        'Authorization': f"Bearer {{CDE_TOKEN}}",
        'accept': 'application/json',
        'Content-Type': 'application/json',
    }

    data = '{ "name": {job_name}, "type": "spark", "retentionPolicy": "keep_indefinitely", "mounts": [ { "dirPrefix": "/", "resourceName": {resource_name} } ], "spark": { "file": {file_name}, "conf": { "spark.pyspark.python": "python3" } }, "schedule": { "enabled": false} }'.format(job_name, resource_name, file_name)

    x = requests.post('http://$%7BCDE_JOB_URL_AWS%7D/jobs', headers=headers, data=data)
    print(x.status_code)


if __name__ == "__main__":
    #Get Token to interact with CDE VC
    tok = set_cde_token()
    #Create CDE Resource for all Spark CDE Jobs
    create_cde_resource(tok, "cde_migration_resource")
    #Upload files to CDE resource
    for file in file_names_list:
        post_files(resource_name, file, tok)
    #Create CDE Jobs from argv
    for job, file in zip(job_names_list, file_names_list):
        post_cde_job(job, resource="cde_migration_resource", file, tok)

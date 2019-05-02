"""Python functions that utilitize the Azure Databricks Create Jobs API and
Databricks CLI to create scheduled Databricks Jobs to be executed with Jobs clusters."""

import sys
import base64
import subprocess
import json
import requests
import argparse
import logging
import yaml
from pprint import pprint

JOB_SRC_PATH = "c:/databricks_repo/Jobs/job_demo.ipynb"
JOB_DEST_PATH = "/notebooks/jobs_demo"
INIT_SCRIPT_SRC_PATH = "c:/databricks_repo/workspace/init.sh"
INIT_SCRIPT_DEST_PATH = "dbfs:/databricks/scripts/"
RSTUDIO_FS_PATH = "dbfs:/databricks/scripts"
WORKSPACE_PATH = "/notebooks"
JOB_JSON_PATH = "c:/databricks_repo/jobs/databricks_new_cluster_job.json"

with open("c:\\databricks_repo\\jobs\\databricks_vars_file.yaml") as config_yml_file:
    databricks_vars = yaml.safe_load(config_yml_file)

JOBSENDPOINT = databricks_vars["databricks-config"]["host"]
DATABRICKSTOKEN = databricks_vars["databricks-config"]["token"]

def create_api_post_args(token, job_jsonpath):
    WORKSPACETOKEN = token.encode("ASCII")
    headers = {'Authorization': b"Basic " +
           base64.standard_b64encode(b"token:" + WORKSPACETOKEN)}
    with open(job_jsonpath) as json_read:
        json_data = json.load(json_read)
    data = json_data
    return headers, data

#Configure token function
def config_databricks_token(token):
    """Configure
        databricks token"""
    try:
        databricks_config_path = "c:/Users/juser/.databrickscfg"
        with open(databricks_config_path, "w") as f:
            clear_file = f.truncate()
            databricks_config = ["[DEFAULT]\n","host = https://eastus.azuredatabricks.net\n","token = {}\n".format(token),"\n"]
            f.writelines(databricks_config)
    except Exception as err:
        logging.debug("Exception occured:", exc_info = True)

def databricks_dir_exists(dir_type, dirpath):
    try:
        if dir_type == "fs":
            subprocess.check_output("databricks fs ls {}".format(dirpath))
        elif dir_type == "workspace":
            subprocess.check_output("databricks workspace ls {}".format(dirpath))
        return True
    except subprocess.CalledProcessError as err:
        logging.debug("Exception occured checking databricks_dir_exists:", exc_info = True)
        # logging.error("Exception occured {}".format(err))
        #pprint(err.output)
        return False

def delete_databricks_dir(dir_type, dirpath):
    try:
        if dir_type == "fs":
            subprocess.check_output("databricks fs rm -r {}".format(dirpath))
        elif dir_type == "workspace":
            subprocess.check_output("databricks workspace rm -r {}".format(dirpath))
    except subprocess.CalledProcessError as err:
        logging.debug("Exception occured with delete_databricks_dir:", exc_info = True)
        pprint(err.output)
        sys.exit(1)

def create_databricks_dir(dir_type, dirpath):
    try:
        if dir_type == "fs":
            subprocess.check_output("databricks fs mkdirs {}".format(dirpath))
        elif dir_type == "workspace":
            subprocess.check_output("databricks workspace mkdirs {}".format(dirpath))
    except subprocess.CalledProcessError as err:
        logging.debug("Exception occured wih create_databricks_dir:", exc_info = True)
        pprint(err.output)
        sys.exit(1)

def copy_databricks_artifacts(dir_type, src_path, dest_path):
    try:
        if dir_type == "fs":
            subprocess.check_output("databricks fs cp {0} {1}".format(src_path, dest_path))
        elif dir_type == "workspace":
            subprocess.check_output("databricks workspace import --format JUPYTER --overwrite --language PYTHON {0} {1}".format(src_path, dest_path))
    except subprocess.CalledProcessError as err:
        logging.debug("Exception occured with copy_databricks_artifacts:", exc_info = True)
        pprint(err.output)
        sys.exit(1)

def copy_job_files():
    """Copy
        Required files to DBFS using databricks-cli"""
    try:
        #demo
        if databricks_dir_exists("fs", RSTUDIO_FS_PATH):
            delete_databricks_dir("fs", RSTUDIO_FS_PATH)
            create_databricks_dir("fs", RSTUDIO_FS_PATH)
        else:
            create_databricks_dir("fs", RSTUDIO_FS_PATH)

        if databricks_dir_exists("workspace", WORKSPACE_PATH):
            delete_databricks_dir("workspace", WORKSPACE_PATH)
            create_databricks_dir("workspace", WORKSPACE_PATH)
        else:
            create_databricks_dir("workspace", WORKSPACE_PATH)

        copy_databricks_artifacts("fs", INIT_SCRIPT_SRC_PATH, INIT_SCRIPT_DEST_PATH)
        copy_databricks_artifacts("workspace", JOB_SRC_PATH, JOB_DEST_PATH)
    except Exception as err:
        logging.debug("Exception occured with copy_job_files:", exc_info = True)
        


def create_job(job_endpoint, header_config, data):
    """Create
        Azure Databricks Spark Notebook Task Job"""
    try:
        #demo
        response = requests.post(
            job_endpoint,
            headers=header_config,
            json=data
        )
        return response
    except Exception as err:
        logging.debug("Exception occured with create_job:", exc_info = True)
        

def run_job(job_id):
    """Use the passed job id to run a job.
      To be used with the create jobs api only, not the run-submit jobs api"""
    try:
        #demo
        subprocess.call("databricks jobs run-now --job-id {}".format(job_id))
    except subprocess.CalledProcessError as err:
        logging.debug("Exception occured with create_job:", exc_info = True)


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-t", "--token", action="store", dest="token", help="Databricks security token for accessing Databricks workspace", required=True, default=False)
    results = parser.parse_args()
    return results

#Driver function
if __name__ == "__main__":
    logging.basicConfig(filename="c:/databricks_repo/jobs/dbricks_job.log", filemode="a", level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
    # arg_results = get_arguments()
    # databricks_token = arg_results.token
    headers, data = create_api_post_args(DATABRICKSTOKEN, JOB_JSON_PATH)
    config_databricks_token(DATABRICKSTOKEN)
    copy_job_files()
    resp = create_job(JOBSENDPOINT, headers, data)
    resp = resp.json() # dict job id object "{'job_id': 9}"
    print("Job ID", resp['job_id'])
    run_job(resp['job_id']) # To be used with the create jobs api only
    

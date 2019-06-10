"""Function calls that
create an Azure Databricks workspace."""

import json
import logging
import yaml
import base64
import requests
import time
from pathlib import Path
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode

DATA_PATH = Path.cwd()
YAML_VARS_FILE = "{}/databricks_workspace_vars.yaml".format(DATA_PATH)
TEMPLATE_PATH = "{}/databricks_premium_workspaceLab.json".format(DATA_PATH)
RESOURCE_GROUP_PARAMS = {"location": "eastus"}
RESOURCE_GROUP_NAME = "RGDatabricks"
JSON_REQUEST_PATH = "{}/deploy_databricks_cluster_restapi.json".format(DATA_PATH)

def read_yaml_vars_file(yaml_file):
    """Read yaml
    file for cred values"""

    # Azure Subscription ID
    with open(yaml_file) as databricks_workspace_file:
        databricks_ws_config = yaml.safe_load(databricks_workspace_file)

    subscription_id = databricks_ws_config["databricks-ws-config"]["subscription_id"]
    clientid = databricks_ws_config["databricks-ws-config"]["clientid"]
    key = databricks_ws_config["databricks-ws-config"]["key"]
    tenantid = databricks_ws_config["databricks-ws-config"]["tenantid"]
    api_endpoint = databricks_ws_config["databricks-ws-config"]["api-endpoint"]

    # Manage Resource Group Parameters
    return subscription_id, clientid, key, tenantid, api_endpoint


def get_auth_credentials(subscriptionid,client_id,client_key,tenant_id):

    # Databricks Service Principal/ Application ID
    CLIENTID = client_id

    # Databricks Application Key
    KEY = client_key

    # Azure Tenant ID
    TENANTID = tenant_id

    credentials = ServicePrincipalCredentials(
        client_id=CLIENTID,
        secret=KEY,
        tenant=TENANTID
    )
    client_obj = ResourceManagementClient(credentials, subscriptionid)
    return client_obj


def deploy_databricks_workspace(client,template_path,resource_group_name,resource_group_params):
    try:
        #test

        # Read template file
        with open(template_path) as template_file:
            template = json.load(template_file)

        # Define template deployment properties
        deployment_properties = {
            'mode': DeploymentMode.incremental,
            'template': template,
        }

        # Create Resource Group and Databricks Workspace resource
        print('Creating Resource Group')
        client.resource_groups.create_or_update(
            resource_group_name, resource_group_params)

        deployment_async_operation = client.deployments.create_or_update(
            resource_group_name,
            'databrickswsdeployment',
            deployment_properties
        )
        print("Beginning the deployment... \n\n")
        # Deploy the template
        deployment_async_operation.wait()
        print("Done deploying!!\n\n")
    except Exception as err:
        logging.debug("Exception occurred:", exc_info=True)


def delete_databricks_resourcegroup(res_group_name, client):
    # Delete the specified ResourceGroup and resources within
    delete_async_operation = client.resource_groups.delete(res_group_name)
    delete_async_operation.wait()
    print("Deleting the defined resource group and resources within..")

def token_console_input():
    """ input databricks
    workspace token """

    ws_token = input("Enter Databricks Token: ")

    return ws_token

def cluster_post_req_args(token, json_request_path):
    #pass
    """ post request
    config """

    WORKSPACETOKEN = token.encode("ASCII")
    headers = {'Authorization': b"Basic " +
           base64.standard_b64encode(b"token:" + WORKSPACETOKEN)}
    with open(json_request_path) as json_request_file:
        json_request = json.load(json_request_file)
    data = json_request
    return headers, data

def create_cluster_req(api_endpoint,headers_config,data):
    """Provision
        new cluster"""
    try:
        response = requests.post(
            api_endpoint,
            headers=headers_config,
            json=data
        )
        return response
    except Exception as err:
        logging.debug("Exception occured with create_job:", exc_info = True)
    
    

#Driver function
if __name__ == "__main__":
    logging.basicConfig(filename="Workspace-DB50\\deploy_dbricks_ws.log", filemode="a", level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
    subscription_id, clientid, key, tenantid, api_endpoint = read_yaml_vars_file(YAML_VARS_FILE)
    client = get_auth_credentials(subscription_id,clientid,key,tenantid)
    deploy_databricks_workspace(client,TEMPLATE_PATH,RESOURCE_GROUP_NAME,RESOURCE_GROUP_PARAMS)
    ws_token = token_console_input()
    print(ws_token)
    headers, data = cluster_post_req_args(ws_token, JSON_REQUEST_PATH)
    time.sleep(30)
    response = create_cluster_req(api_endpoint,headers,data)

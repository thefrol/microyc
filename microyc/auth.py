# version 0.1
import os


def create_headers(token): #generates auth header - dictionary
    return {"Authorization":"Bearer %s"%token}

def env(variable_name:str):
    return os.environ[variable_name]

def first_of_env(*name_list):
    for name in name_list:
        if name in os.environ:
            return os.environ[name]


def get_token():
    if "TOKEN" in os.environ:
        return os.environ["TOKEN"]
    if global_token:
        return global_token


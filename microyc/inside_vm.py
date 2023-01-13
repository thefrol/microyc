google_compute_metadata_url='http://169.254.169.254/computeMetadata/v1/'
token_url='http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token'
from microyc.objects import VirtualMachine
import requests

def __do_request(url, recursive=True):

    header={
        'Metadata-Flavor':'Google'
    }

    payload={
        'recursive': 'true' if recursive else 'false',
        'alt':'json'
    }

    response=requests.get(
        url=url,
        params=payload,
        headers=header
    )
    return response.json()

class VirtualMachineMetadata:

    def __init__(self, metadata):
        self.__metadata=metadata
    
    @property
    def id(self):
        return self.__metadata['instance']['id']

    @property
    def name(self):
        return self.__metadata['instance']['name']


    

def get_metadata():

    return VirtualMachineMetadata(
            metadata=__do_request(url=google_compute_metadata_url)
        )

def get_my_id():
    return get_metadata().id

@property
def id():
    return get_my_id()

@property
def name():
    return get_my_name()

@property
def get_my_name():
    return get_metadata().name

def get_token():
    return __do_request(url=token_url)['access_token']


def get_my_vm():
    meta=get_metadata()
    return VirtualMachine(
        instance_id=meta.id,
        iam_token=get_token()
        )

def suicide():
    get_my_vm().delete()
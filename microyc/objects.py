# version 0.2.0

import requests
import boto3
import boto3.exceptions
import botocore #для генерации ссылок, при загрузке файла в бакет
import microyc.auth
import microyc.utils

vm_instance_env="VM_INSTANCE"
queue_env="QUEUE"
fifo_env='FIFO'
bucket_env="BUCKET"
db_document_endpoint_env="DOCUMENT_ENDPOINT"
db_table_name_env="TABLE"

class VirtualMachine:
    """doc"""
    service_url="https://compute.api.cloud.yandex.net/compute/v1/instances/"
    __stop_command="stop"
    __start_command="start"

    def __init__(self, instance_id:str=None,iam_token:str=None):
        global vm_instance_env
        if instance_id:
            self.instance=instance_id
        else:
            microyc.auth.env(vm_instance_env)
        if iam_token:
            self.token=iam_token
        else:
            self.token=microyc.auth.get_token()

    def __change_state(self, state):
        if not state in [self.__start_command,self.__stop_command]:
            print("state must be 'start' or 'stop'")
            return 

        header=microyc.auth.create_headers(self.token)
        caller_url=self.service_url+self.instance+":"+state

        response=requests.post(caller_url, headers=header)

        #если машина уже выключена, то при попытке остановить ещё раз он присылает код 200 - выполнено
        #если машина уже запущена, и мы пытаемся ее запустить, то ошибка
    
        if response.status_code==200:
            print("Success!")
        if response.status_code==401:
            raise Exception("Authentication error. "+str(response.content))
        if response.status_code==400:
            raise Exception("Bad request. Maybe machine already running."+str(response.content))

    def start(self):
        self.__change_state(self.__start_command)
    def stop(self):
        self.__change_state(self.__stop_command)

    def delete(self):
        header=microyc.auth.create_headers(self.token)
        caller_url=self.service_url+self.instance
        response=requests.delete(caller_url, headers=header)
        if response.status_code==200:
            print("Success!")
        else:
            print(f"Error happened: {response.content}")


class MessageQueue:
    """только для стандартный очередей"""
    __service_url='https://message-queue.api.cloud.yandex.net'
    __region_name='ru-central1'
    def __init__(self, QueueName:str=None,aws_key=None,aws_secret=None):
        global queue_env
        if QueueName:
            self.queue_name=QueueName
        else:
            self.queue_name=microyc.auth.env(queue_env)
        sqs = boto3.resource(
            service_name='sqs',
            region_name=self.__region_name,
            endpoint_url=self.__service_url,
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret)
        self.queue= sqs.get_queue_by_name(QueueName=self.queue_name);
    def send(self,Body):
        """text or json"""
        self.queue.send_message(MessageBody=str(Body))
    def recv_one_and_delete(self, WaitTime=10):
        response=self.queue.receive_messages(MaxNumberOfMessages=1,WaitTimeSeconds=WaitTime)#//.get('Messages')
        if len(response)>0:
            #вообще бы надо проверить чтобы он получал по одному эти сообщения
            ret= response[0].body
            response[0].delete()
            return ret
        else:
            return None

class FifoQueue:
    """только для Очередей Фифо 
    !!! Обязательно должно быть включен параметр 'дедупликация сообщений'
    в настройках очереди в яндекс облаке """
    __service_url='https://message-queue.api.cloud.yandex.net'
    __region_name='ru-central1'
    def __init__(self, QueueName:str=None,aws_key=None,aws_secret=None):
        global queue_env,fifo_env
        if QueueName:
            self.queue_name=QueueName
        else:
            self.queue_name=microyc.auth.first_of_env(fifo_env,queue_env)
        if self.queue_name==None:
            raise Exception("Cant find name for queue. Specify in QuequName parameter. Or in enviromental variables FIFO or QUEUE")
        sqs = boto3.resource(
            service_name='sqs',
            region_name=self.__region_name,
            endpoint_url=self.__service_url,
            aws_access_key_id=aws_key,
            aws_secret_access_key=aws_secret)
        self.queue= sqs.get_queue_by_name(QueueName=self.queue_name);
    def send(self,Body,GroupId="default"):
        """text or json"""
        self.queue.send_message(MessageBody=str(Body),MessageGroupId=GroupId)
    def recv_one_and_delete(self, GroupId="default",WaitTime=10):
        response=self.queue.receive_messages(MaxNumberOfMessages=1,WaitTimeSeconds=WaitTime)#//.get('Messages')
        if len(response)>0:
            #вообще бы надо проверить чтобы он получал по одному эти сообщения
            ret= response[0].body
            response[0].delete()
            return ret
        else:
            return None

class Bucket:
    __service_url='https://storage.yandexcloud.net'
    __service_name="s3"
    __storage_classes= ["COLD","STANDARD"]
    def __init__(self, BucketName=None, aws_key=None,aws_secret=None):
        global bucket_env
        if BucketName:
            self.bucket_name=BucketName
        else:
            self.bucket_name=microyc.auth.env(bucket_env)

        session = boto3.session.Session()
        self.__client = session.client(service_name          = self.__service_name, 
                                       endpoint_url          = self.__service_url,
                                       aws_access_key_id     = aws_key,
                                       aws_secret_access_key = aws_secret,
                                       
                                       )

        config = botocore.client.Config(signature_version=botocore.UNSIGNED)
        config.signature_version = botocore.UNSIGNED

        session = boto3.session.Session()
        self.__unsigned_client = session.client(service_name          = self.__service_name, 
                                                endpoint_url          = self.__service_url,
                                                aws_access_key_id     = aws_key,
                                                aws_secret_access_key = aws_secret,
                                                config                = config) ## этот клиент создает ссылки
    def put(self,Body,Path,StorageClass="STANDARD"):
        """возвращает ссылку на объект"""
        if not StorageClass in self.__storage_classes:
            print("WARN: storage class should be on of "+str(self.__storage_classes))

        self.__client.put_object(Bucket       = self.bucket_name,
                                 Key          = Path,
                                 Body         = Body,
                                 StorageClass = StorageClass)

        return self.__get_link(Path)
    def upload(self,Path,Key=None):
        filename=microyc.utils.get_filename(Path)
        if Key:
            upload_key=Key
        else:
            upload_key=filename
        self.__client.upload_file(Bucket=self.bucket_name,Filename=Path,Key=upload_key)
        return filename
    def upload_unique(self,Path):
        filename=microyc.utils.generate_new_filename(Path)
        self.__client.upload_file(Bucket=self.bucket_name,Filename=Path,Key=filename)
        return filename
    def download_fileobj(self, fileobj,Key:str,Directory:str="."):
        #filepath=microyc.utils.combine_path(Directory,Key)
        #data=open(filepath, 'wb')
        self.__client.download_fileobj(Bucket=self.bucket_name,Key=Key, Fileobj=fileobj)
    def head(self, Key):
        try:
            return self.__client.head_object(Bucket=self.bucket_name,Key=Key)
        except boto3.exceptions.botocore.exceptions.ClientError as ex:
                if ex.response['Error']['Code'] == '404':
                    return None
                else:
                    raise ex
    def exists(self,Key):
        return self.head(Key) != None
    def download_file(self,Key:str,Directory:str="."):
        #тут явно что-то не то, что за директори?
        filepath=microyc.utils.combine_path(Directory,Key)
        self.__client.download_file(Bucket=self.bucket_name,Key=Key, Filename=filepath)
        return filepath
    def get(self, Path:str):
        get_object_response = self.__client.get_object(Bucket = self.bucket_name,
                                            Key    = Path)
        return get_object_response['Body'].read()
    def delete(self, Path:str):
        response = self.__client.delete_object(Bucket=self.bucket_name,
                                    Key=Path)
        return response
    def list(self, MaxKeys=1000):
        """Возвращает не более тысячи значений"""
        response = self.__client.list_objects(Bucket=self.bucket_name, MaxKeys=MaxKeys)
        keys=[content["Key"] for content in response["Contents"]]
        return keys
    def objects_info(self, MaxKeys=1000):
        """Возвращает не более тысячи значений"""
        response = self.__client.list_objects(Bucket=self.bucket_name, MaxKeys=MaxKeys)
        return response["Contents"]
    def copy(self,FromPath:str,ToPath:str):
        """now working"""
        response=self.__client.copy_object(Bucket=self.bucket_name,
                                           CopySource=FromPath,
                                           Key=ToPath,
                                           StorageClass="STANDARD")
        return self.__get_link(ToPath)


    def __get_link(self,Path):
        return  self.__unsigned_client.generate_presigned_url(ClientMethod = 'get_object',
                                                              ExpiresIn    = 0,
                                                              Params       = {'Bucket' : self.bucket_name,
                                                                              'Key'    : Path})

class DocumentTable:
    """Документная таблица YDB"""
    __aws_service_name="dynamodb"
    def __init__(self, TableName:str=None,DocumentEndpoint:str=None,aws_key:str=None,aws_secret:str=None):
        global db_document_endpoint_env
        global db_table_name_env

        if TableName:
            self.table_name=TableName
        else:
            self.table_name=microyc.auth.env(db_table_name_env)

        if DocumentEndpoint:
            self.document_endpoint=DocumentEndpoint
        else:
            self.document_endpoint=microyc.auth.env(db_document_endpoint_env)

        __client = boto3.resource(service_name          = self.__aws_service_name,
                                  endpoint_url          = self.document_endpoint,
                                  aws_access_key_id     = aws_key,
                                  aws_secret_access_key = aws_secret,
                                  region_name='ru-central1')

        self.__table = __client.Table(self.table_name)
        self.schema=self.__table.key_schema
        self.keys={attr["KeyType"]:attr["AttributeName"] for attr in self.schema}
        self.hash_key=self.keys["HASH"]
        if "RANGE" in self.keys:
            self.range_key=self.keys["RANGE"]
    def scan(self):
        req= self.__table.scan();
        return req["Items"];
    def put(self,Item:dict):
        response = self.__table.put_item(Item = Item)
        return response
    def update(self, Key:dict,AttributeUpdates:dict):
        update_expression={key: {'Value':AttributeUpdates[key],
                                 'Action':'PUT'} 
                                  for key in AttributeUpdates}
        return self.__table.update_item(Key=Key,AttributeUpdates=update_expression)
    def get_by_key(self,KeyName:str,KeyValue:str):
        response = self.__table.get_item(Key={KeyName:KeyValue})
        if "Item" in response:
            return response['Item']
        else:
           return None
    

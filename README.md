# microyc

minimalistic sdk for operating with yandex.cloud

project currently abandonned for nanoyc project, but still works fine

## Authorization

for Amazon services autorize like with boto3

Read more at https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html

### Use .aws file

store it in ~/.aws with credentials~/.aws/credentials

### Enviromental variables (recomended)

`AWS_ACCESS_KEY_ID` - The access key for your AWS account.
`AWS_SECRET_ACCESS_KEY` - The secret key for your AWS account.

to get this key generate a new static key for your service account at yandex cloud console


## using Object storage

predefine `BUCKET` environment variable with name of bucket

    from microyc import Bucket

    b=Bucket()  # or b=Bucket(BucketName='my-bucket')
    b.upload('my_file.txt')

## more 

u can user FifoQuequest, start virtual machines, use DocumentDatabase  and even suicide a turned virtual machine

check `objects.py` file

## revision history

0.0.6 fixed fifo recv() issue
0.0.5 Bug fixes and Bucket.upload_unique
0.0.4 added FifoQueue

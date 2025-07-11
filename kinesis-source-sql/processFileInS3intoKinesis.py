import json
import boto3
import uuid

s3 = boto3.client('s3')
kinesis = boto3.client('kinesis')

def lambda_handler(event, context):

    # with open ("s3://vinod-apache-flink/kinesis-based/FUTURES_TRADES.txt","r") as futures_trades:
    #     for line in futures_trades:
    #         print(line)

    get_object_response = s3.get_object(
        Bucket='vinod-apache-flink',
        Key='kinesis-based/FUTURES_TRADES.txt'
    )

    # print('The get object response is: ', get_object_response)

    get_object_response_body = get_object_response['Body'].read().decode('utf-8')

    # print('The get object response body is: ', get_object_response_body)

    # print('The get object response body type is: ', type(get_object_response_body))

    lines = get_object_response_body.split('\n')
    trades = []
    i = 0
    j = 1

    for line in lines:
        
        kwargs = {'Data': json.dumps(line).encode('utf-8'), 'PartitionKey': str(uuid.uuid4())}
        trades.append(kwargs)
        i = i + 1

        if (i == 20):
            kinesis.put_records(
                StreamName='vinod-apache-flink-kinesis-stream',
                Records=trades
            )
            trades = []
            i = 0

            print('Completed batch number: ' + str(j))
            j = j + 1
       
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

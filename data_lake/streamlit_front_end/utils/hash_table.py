import boto3
from boto3.dynamodb.conditions import Key

def clear_hash_table():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('PK_Hash_Index')
    
    # Scan all items to get keys
    response = table.scan(
        ProjectionExpression='PK_Hash'
    )
    
    # Batch delete in chunks of 25 (DynamoDB limit)
    with table.batch_writer() as batch:
        for item in response['Items']:
            batch.delete_item(Key={'PK_Hash': item['PK_Hash']})
    
    # Handle pagination if results exceed 1MB
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ProjectionExpression='PK_Hash',
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        with table.batch_writer() as batch:
            for item in response['Items']:
                batch.delete_item(Key={'PK_Hash': item['PK_Hash']})
    
    print("All items deleted from PK_Hash_Index")

clear_hash_table()
import boto3

def lambda_handler(event, context):
    # Initialize DynamoDB client
    dynamodb = boto3.client('dynamodb')

    # Define the table name
    table_name = 'indexed_data'

    # Extract new unique_uid from the event
    new_user_id = event.get('new_user_id')
    # new_user_phone_number = event.get('new_user_phone_number')

    try:
        # Retrieve record from DynamoDB table
        response = dynamodb.query(
            TableName=table_name,
            IndexName = 'user_id-index',
            KeyConditionExpression='#user_id = :user_id',  # Added user_id to the KeyConditionExpression
            ExpressionAttributeNames={'#user_id': 'user_id'},  # ExpressionAttributeNames should match the primary key attribute name
            ExpressionAttributeValues={':user_id': {'S': event['user_id']}}
        )
        
        # Check if records are found
        if 'Items' in response and len(response['Items']) > 0:
            num_records_found = len(response['Items'])
            print("Number of records found with user_id {}: {}".format(event['user_id'], num_records_found))
            
            # Create new records with updated user_id
            for item in response['Items']:
                item['user_id'] = {'S': new_user_id}
                # item['user_phone_number'] = {'S': new_user_phone_number}
                response = dynamodb.put_item(
                    TableName=table_name,
                    Item=item
                )
            print("Number of records created with new_user_id {}: {}".format(new_user_id, num_records_found))
            
            return {
                'statusCode': 200,
                'body': 'Records updated and created successfully'
            }
        else:
            print("No records found with user_id:", event['user_id'])
            return {
                'statusCode': 404,
                'body': 'No records found'
            }
    except Exception as e:
        print("Error:", e)
        return {
            'statusCode': 500,
            'body': 'Error: {}'.format(e)
        }

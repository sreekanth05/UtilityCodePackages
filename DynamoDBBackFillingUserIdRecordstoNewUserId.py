## This code writes the records from old user_id to new_user_id

import boto3

def lambda_handler(event, context):
    # Initialize DynamoDB client
    dynamodb = boto3.client('dynamodb')

    # Define the table name
    table_name = 'indexed_data'

    # Extract new unique_uid from the event
    new_user_id = event.get('new_user_id')

    try:
        # Initialize the query parameters
        query_params = {
            'TableName': table_name,
            'IndexName': 'user_id-index',
            'KeyConditionExpression': '#user_id = :user_id',
            'ExpressionAttributeNames': {'#user_id': 'user_id'},
            'ExpressionAttributeValues': {':user_id': {'S': event['user_id']}}
        }

        all_items = []
        
        # Retrieve records from DynamoDB table with pagination
        while True:
            response = dynamodb.query(**query_params)
            
            if 'Items' in response:
                all_items.extend(response['Items'])
            
            # Check if there's more data to be fetched
            if 'LastEvaluatedKey' in response:
                query_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
            else:
                break
        
        num_records_found = len(all_items)
        print("Number of records found with user_id {}: {}".format(event['user_id'], num_records_found))

        if num_records_found > 0:
            # Create new records with updated user_id
            for item in all_items:
                item['user_id'] = {'S': new_user_id}
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




## Manual Payload Event


# {
#   "user_id": "6746df08-c5a6-45",
#   "new_user_id": "030341a4-9e38-43"
# }

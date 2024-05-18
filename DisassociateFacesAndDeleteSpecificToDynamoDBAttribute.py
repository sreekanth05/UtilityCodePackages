import json
import boto3

# Initialize the Rekognition client
rekognition = boto3.client('rekognition')
dynamodb = boto3.client('dynamodb')

def get_face_data_from_dynamodb(user_id):
    # Assuming you have a DynamoDB table named 'indexed_data' with a global secondary index named 'user_id_index'
    response = dynamodb.query(
        TableName='indexed_data',
        IndexName='user_id-index',
        KeyConditionExpression='user_id = :uid',
        ExpressionAttributeValues={
            ':uid': {'S': user_id}
        }
    )
    return [(item['face_id']['S'], item['folder_name']['S']) for item in response['Items']]

def disassociate_faces_from_collection(collection_id, face_ids, user_id):
    for face_id in face_ids:
        response = rekognition.disassociate_faces(
            CollectionId=collection_id,
            UserId=user_id,
            FaceIds=[face_id]  # Pass face_id as a list
        )
        print(f"Disassociated face {face_id} from user {user_id}")
    return True

def delete_faces_from_collection(collection_id, user_id):
    # Get all face data associated with the user ID from DynamoDB
    face_data = get_face_data_from_dynamodb(user_id)
    
    # Filter face data based on folder name
    filtered_face_data = [(face_id, folder_name) for face_id, folder_name in face_data if folder_name == 'Venky_Spandana_Reception_06022022']
    
    # Extract face IDs
    face_ids = [face_id for face_id, _ in filtered_face_data]
    
    # Disassociate the face IDs from the collection
    disassociate_faces_from_collection(collection_id, face_ids, user_id)
    
    # Delete the faces
    response = rekognition.delete_faces(CollectionId=collection_id, FaceIds=face_ids)
    
    return response['DeletedFaces']

def lambda_handler(event, context):
    # Collection ID where faces are stored
    collection_id = 'FlashbackUserDataCollection'
    
    # User IDs provided in the event
    user_ids = event.get('user_ids', [])
    
    for user_id in user_ids:
        print(f"Processing User ID: {user_id}")
        
        # Delete face IDs associated with the user from the specified folder
        deleted_faces = delete_faces_from_collection(collection_id, user_id)
        total_faces_deleted = len(deleted_faces)
        
        print(f"User ID: {user_id}")
        print(f"Total FaceIDs Deleted: {total_faces_deleted}")
        print("------------------------")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Deleted face IDs successfully'
        })
    }

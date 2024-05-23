# This Utility Lambda is for the disassociating the faces for the newUsers created, deleting the face_ids and user_ids.

import json
import boto3

# Initialize the Rekognition client
rekognition = boto3.client('rekognition')
dynamodb = boto3.client('dynamodb')

def get_face_ids_from_dynamodb(user_id):
    # Assuming you have a DynamoDB table named 'indexed_data' with a global secondary index named 'user_id_index'
    response = dynamodb.query(
        TableName='indexed_data',
        IndexName='user_id-index',
        KeyConditionExpression='user_id = :uid',
        ExpressionAttributeValues={
            ':uid': {'S': user_id}
        }
    )
    return [item['face_id']['S'] for item in response['Items']]

def disassociate_faces_from_collection(collection_id, user_id, face_ids):
    # Disassociate the faces one by one
    for face_id in face_ids:
        response = rekognition.disassociate_faces(
            CollectionId=collection_id,
            UserId=user_id,
            FaceIds=[face_id]  # Pass face_id as a list
        )
        print(f"Disassociated face {face_id} from user {user_id}")
    return True

def delete_user_from_collection(collection_id, user_id):
    print(f"Deleting user: {user_id} from collection: {collection_id}")
    try:
        response = rekognition.delete_user(
            CollectionId=collection_id,
            UserId=user_id
        )
        print(f"Deleted user {user_id} from collection {collection_id}")
        return response
    except ClientError as e:
        print(f"Failed to delete user {user_id} from collection {collection_id}: {e}")
        raise


def lambda_handler(event, context):
    # Collection ID where faces are stored
    collection_id = 'FlashbackUserDataCollection'
    
    # User IDs provided in the event
    user_ids = event.get('user_ids', [])
    
    for user_id in user_ids:
        print(f"Processing User ID: {user_id}")
        
        # Get face IDs associated with the user ID from DynamoDB
        face_ids = get_face_ids_from_dynamodb(user_id)
        
        total_faces_found = len(face_ids)
        total_faces_disassociated = 0
        
        # Disassociate face IDs from the Rekognition collection
        if face_ids:
            try:
                print(f"Disassociating {total_faces_found} faces from collection for User ID: {user_id}")
                disassociate_faces_from_collection(collection_id, user_id, face_ids)
                total_faces_disassociated += len(face_ids)
            except Exception as e:
                print(f"An error occurred during disassociation for User ID: {user_id}: {e}")
        
        # Delete user ID from the collection
        try:
            print(f"Deleting {total_faces_found} faces from collection for User ID: {user_id}")
            deleted_faces = delete_user_from_collection(collection_id, user_id)
        except Exception as e:
            print(f"An error occurred during deletion for User ID: {user_id}: {e}")
        
        print(f"User ID: {user_id}")
        print(f"Total FaceIDs Found: {total_faces_found}")
        print(f"Total FaceIDs Disassociated: {total_faces_disassociated}")
        print("------------------------")

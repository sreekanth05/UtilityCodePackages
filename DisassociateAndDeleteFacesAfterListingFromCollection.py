import boto3
from botocore.exceptions import ClientError

# Initialize the Rekognition client
rekognition = boto3.client('rekognition')

def list_faces_in_collection(collection_id, user_id):
    face_ids = []
    try:
        response = rekognition.list_faces(
            CollectionId=collection_id,
            MaxResults=1000  # Adjust as needed
        )
        while True:
            faces = response.get('Faces', [])
            for face in faces:
                if face.get('UserId') == user_id:
                    face_ids.append(face['FaceId'])
            if 'NextToken' in response:
                response = rekognition.list_faces(
                    CollectionId=collection_id,
                    NextToken=response['NextToken'],
                    MaxResults=1000
                )
            else:
                break
        return face_ids
    except ClientError as e:
        print(f"Error listing faces for user {user_id} in collection {collection_id}: {e}")
        raise

def disassociate_faces_from_collection(collection_id, user_id, face_ids):
    try:
        for face_id in face_ids:
            rekognition.disassociate_faces(
                CollectionId=collection_id,
                UserId=user_id,
                FaceIds=[face_id]
            )
            print(f"Disassociated face {face_id} from user {user_id}")
        return True
    except ClientError as e:
        print(f"Error disassociating faces for user {user_id}: {e}")
        raise

def delete_user_from_collection(collection_id, user_id):
    try:
        response = rekognition.delete_user(
            CollectionId=collection_id,
            UserId=user_id
        )
        print(f"Deleted user {user_id} from collection {collection_id}")
        return response
    except ClientError as e:
        print(f"Error deleting user {user_id} from collection {collection_id}: {e}")
        raise

def lambda_handler(event, context):
    collection_id = 'FlashbackUserDataCollection'
    user_ids = event.get('user_ids', [])
    
    for user_id in user_ids:
        print(f"Processing User ID: {user_id}")
        
        # Get face IDs associated with the user ID from the collection
        try:
            face_ids = list_faces_in_collection(collection_id, user_id)
            print(f"Found {len(face_ids)} face IDs for user {user_id}")
        except Exception as e:
            print(f"An error occurred while listing faces for User ID: {user_id}: {e}")
            continue
        
        # Disassociate face IDs from the Rekognition collection
        if face_ids:
            try:
                disassociate_faces_from_collection(collection_id, user_id, face_ids)
            except Exception as e:
                print(f"An error occurred during disassociation for User ID: {user_id}: {e}")
        
        # Delete user ID from the collection
        try:
            delete_user_from_collection(collection_id, user_id)
        except Exception as e:
            print(f"An error occurred during deletion for User ID: {user_id}: {e}")
        
        print(f"Completed processing for User ID: {user_id}")
        print("------------------------")


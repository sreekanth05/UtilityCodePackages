import json
import boto3
from io import BytesIO
from PIL import Image
from datetime import datetime
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from urllib.parse import urlparse

# Initialize boto3 clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# DynamoDB table names
INDEXED_DATA_TABLE = 'indexed_data'
REKOGNITION_USERS_DATA_TABLE = 'RekognitionUsersData'
#bucket_name = 'flashbackusercollection'

# S3 bucket to store cropped images
S3_BUCKET = 'rekognitionuserfaces'

def store_face_thumbnail(img, S3_BUCKET, bounding_box, user_id):
    try:
        print(f"Cropping thumbnail with bounding box metrics")
        left = max(0, int(bounding_box['Left'] * img.width) - 25)
        top = max(0, int(bounding_box['Top'] * img.height) - 25)
        right = min(img.width, int((bounding_box['Left'] + bounding_box['Width']) * img.width) + 25)
        bottom = min(img.height, int((bounding_box['Top'] + bounding_box['Height']) * img.height) + 25)
        thumbnail = img.crop((left, top, right, bottom))
        
        # Resize if the image is too large
        max_size_bytes = 5 * 1024 * 1024  # 5 MB
        thumbnail_buffer = BytesIO()
        thumbnail.save(thumbnail_buffer, format='JPEG')
        thumbnail_buffer.seek(0)
        if len(thumbnail_buffer.getvalue()) > max_size_bytes:
            thumbnail.thumbnail((1024, 1024))
            thumbnail_buffer = BytesIO()
            thumbnail.save(thumbnail_buffer, format='JPEG')
            thumbnail_buffer.seek(0)
        
        thumbnail_key = f"thumbnails/{user_id}.jpg"
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=thumbnail_key,
            Body=thumbnail_buffer,
            ContentType='image/jpeg'
        )
        thumbnail_url = f"https://{S3_BUCKET}.s3.amazonaws.com/{thumbnail_key}"
        return thumbnail_url
    except Exception as e:
        print(f"An error occurred while storing face thumbnail: {e}")
    return None

def store_user_data(user_id, img, bounding_box):
    try:
        print(f"Found a valid item, initiating thumbnail cropping")
        face_thumbnail_url = store_face_thumbnail(img, S3_BUCKET, bounding_box, user_id)
        record_creation_timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        item = {
            'user_id': user_id,
            'face_url': face_thumbnail_url,
            'recorded_timestamp(UTC)': record_creation_timestamp
        }
        dynamodb.Table(REKOGNITION_USERS_DATA_TABLE).put_item(Item=item)
        print(f"Stored user data: UserId={user_id}, FaceURL={face_thumbnail_url}")
    except Exception as e:
        print(f"An error occurred while storing user data: {e}")

def is_valid_entry(item):
    try:
        occluded_value = item.get('FaceOccluded_Value', None)
        confidence = item.get('Confidence', 0)
        pose = json.loads(item.get('Pose', '{}'))
        roll = pose.get('Roll', 0)
        yaw = pose.get('Yaw', 0)
        pitch = pose.get('Pitch', 0)
        eyes_open_value = item.get('EyesOpen_Value', None)
        eyes_open_confidence = item.get('EyesOpen_Confidence', 0)
        quality = json.loads(item.get('Quality', '{}'))
        brightness = quality.get('Brightness', 0)
        sharpness = quality.get('Sharpness', 0)

        if occluded_value is None or eyes_open_value is None:
            return False

        print(f"Occluded value: {occluded_value}, confidence: {confidence}, eyes_open: {eyes_open_value} with eyes_open_confidence: {eyes_open_confidence} pose: [Roll: {roll}, Pitch: {pitch}, Yaw: {yaw}], brightness: {brightness} and sharpness: {sharpness}")

        return (
            not occluded_value and
            confidence > 98 and
            eyes_open_value and
            eyes_open_confidence > 95 and
            brightness > 40 and
            sharpness > 18 and
            (-20.0 <= roll <= 20.0) and
            (-15.0 <= yaw <= 15.0) and
            (-15.0 <= pitch <= 15.0)
        )
    except KeyError as e:
        print(f"Key error: {e}")
        return False
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return False


def lambda_handler(event, context):
    user_ids = event.get('user_ids', [])
    
    for user_id in user_ids:
        print(f"Processing the userId: {user_id}")
        # Check if user_id is present in RekognitionUsersData table
        try:
            rekognition_users_data_table = dynamodb.Table(REKOGNITION_USERS_DATA_TABLE)
            user_response = rekognition_users_data_table.get_item(Key={'user_id': user_id})
            if 'Item' in user_response:
                # Skip if user_id is already present
                print(f"The user is already present in the RekognitionUsersData, Hence, skipping to next userId")
                continue
        except ClientError as e:
            # Handle exception if needed
            print(f"An error occurred while checking user_id in RekognitionUsersData: {e}")
            continue
        
        folder_name = 'Sithara_Thadem_Birthaday_09062024'
        # Query the indexed_data table for the user_id
        indexed_data_table = dynamodb.Table(INDEXED_DATA_TABLE)
        try:
            response = indexed_data_table.query(
                IndexName='folder_name-user_id-index',
                KeyConditionExpression=Key('user_id').eq(user_id) & Key('folder_name').eq(folder_name)
            )
            
            for item in response['Items']:
                # Check if the current entry meets the criteria
                if is_valid_entry(item):
                    # Fetch the image and crop the face using bounding box
                    image_url = item['s3_url']
                    bounding_box = item['bounding_box']
                    try:
                        parsed_url = urlparse(image_url)
                        bucket_name = parsed_url.netloc.split('.')[0]
                        key = parsed_url.path.lstrip('/')
                        print(f"processing the image: {key} from the bucket {bucket_name}")
                        image_buffer = BytesIO(s3.get_object(Bucket=bucket_name, Key=key)['Body'].read())
                        with Image.open(image_buffer) as img:
                            store_user_data(user_id, img, bounding_box)
                    except Exception as e:
                        print(f"An error occurred while processing image: {e}")
                    break  # Stop processing once a valid entry is found
        except ClientError as e:
            print(f"An error occurred while querying indexed_data table for user_id {user_id}: {e}")
            continue

    # return {
    #     'statusCode': 200,
    #     'body': json.dumps('Processing completed successfully.')
    # }


#Manual payload
# {
#   "user_ids": ["user1", "user2"]
# }

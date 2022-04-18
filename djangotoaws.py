import boto3
import dynamodbgeo
import uuid
from decimal import Decimal
dynamodb = boto3.client('dynamodb', region_name='ap-southeast-1')
config = dynamodbgeo.GeoDataManagerConfiguration(dynamodb, 'test')
geoDataManager = dynamodbgeo.GeoDataManager(config)
config.hashKeyLength = 3
def createtable():
    # Use GeoTableUtil to help construct a CreateTableInput.
    table_util = dynamodbgeo.GeoTableUtil(config)
    create_table_input=table_util.getCreateTableRequest()

    #tweaking the base table parameters as a dict
    create_table_input["ProvisionedThroughput"]['ReadCapacityUnits']=5

    # Use GeoTableUtil to create the table
    table_util.create_table(create_table_input)

def putitem():
    PutItemInput = {
        'Item': {
            'Restaurent':{'S': "(Far)"},
            'Area': {'S': "Rampura"},
            'City': {'S': "Dhaka"},


        },
        'ConditionExpression': "attribute_not_exists(hashKey)"
        # ... Anything else to pass through to `putItem`, eg ConditionExpression

    }
    geoDataManager.put_Point(dynamodbgeo.PutPointInput(
        dynamodbgeo.GeoPoint(23.7613587,90.4329056),  # latitude then latitude longitude
        str(uuid.uuid4()),  # Use this to ensure uniqueness of the hash/range pairs.
        PutItemInput  # pass the dict here
    ))


# Querying 95 meter from the center point (36.879131, 10.243057)
def radiussearch():
    QueryRadiusInput = {
        "FilterExpression": "Area = :val1",
        "ExpressionAttributeValues": {
            ":val1": {"S": "Rampura"},
        },

    }

    result = geoDataManager.queryRadius(
        dynamodbgeo.QueryRadiusRequest(
            dynamodbgeo.GeoPoint(23.7635783, 90.4266944),  # center point
            1000, QueryRadiusInput, sort=True))

    print(result)
def scan_first_and_last_names():
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('geo_test')

    resp = table.scan(ProjectionExpression="Restaurent")


    print(table.scan())

def get_item():
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('geo_test')

    resp = table.get_item(
        Key={
            'geohash':  3987312442795951353
        }
    )

    if 'Item' in resp:
        print(resp['Item'])
def deletetable():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('geo-test')
    table.delete()
radiussearch()
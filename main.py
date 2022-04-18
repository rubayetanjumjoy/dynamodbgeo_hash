import boto3
import dynamodbgeo
import uuid
import psycopg2
from psycopg2 import Error
from decimal import Decimal



#postgres
dynamodb = boto3.client('dynamodb', region_name='ap-southeast-1')
config = dynamodbgeo.GeoDataManagerConfiguration(dynamodb, 'geo2')
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


def putitem(id,distance_in_meters,business_name,place_name,holding_number,road_name_number,super_sub_area,sub_area,
            popularity_ranking,pType,longitude,latitude,
             Address, area,city,subType,uCode,postCode,unions,sub_district,district,thana,ST_AsText,ST_AsGeoJSON,
            additional,images,data_source,bit):
    PutItemInput = {
        'Item': {
            'id': {'S': str(id)},
            'distance_in_meters': {'S': str(distance_in_meters)},

            'business_name': {'S': str(business_name)},
            'place_name': {'S': str(place_name)},
            'holding_number': {'S': str(holding_number)},
            'road_name_number': {'S': str(road_name_number)},
            'super_sub_area': {'S': str(super_sub_area)},
            'sub_area': {'S': str(sub_area)},
            'popularity_ranking': {'S': str(popularity_ranking)},
            'pType': {'S': str(pType)},
            'longitude': {'S': str(longitude)},
            'latitude': {'S': str(latitude)},
            'Address': {'S': str(Address)},
            'area': {'S': str(area)},
            'city': {'S': str(city)},
            'subType': {'S':str(subType) },
            'uCode': {'S':str(uCode) },
            'postCode': {'S':str(postCode) },
            'unions': {'S':str(unions) },

            'sub_district': {'S':str(sub_district) },
            'district': {'S':str(district) },
            'thana': {'S': str(thana)},
            'ST_AsText(location)': {'S':str(ST_AsText) },
            'ST_AsGeoJSON(bounds)': {'S':str(ST_AsGeoJSON) },
            'additional': {'S': str(additional)},
            'images': {'S': str(images)},
            'data_source': {'S': str(data_source)},
            'bit': {'S': str(bit)},


        },
        'ConditionExpression': "attribute_not_exists(hashKey)"
        # ... Anything else to pass through to `putItem`, eg ConditionExpression

    }
    geoDataManager.put_Point(dynamodbgeo.PutPointInput(
        dynamodbgeo.GeoPoint(float(latitude), float(longitude)),  # latitude then latitude longitude
        str(uuid.uuid4()),  # Use this to ensure uniqueness of the hash/range pairs.
        PutItemInput  # pass the dict here
    ))
def insertjsondata():
    import json
    f = open("bashundhara04.txt", "r")
    data = json.load(f)
    count = 0
    for d in data:
        """(id,distance_in_meters,business_name,place_name,holding_number,road_name_number,super_sub_area,sub_area,
            popularity_ranking,pType,longitude,latitude,
             Address, area,city,subType,uCode,postCode,unions,sub_district,district,thana,ST_AsText,ST_AsGeoJSON,
            additional,images,data_source,bit):"""
        print(d[0])
        count += 1
        putitem(d[0]['id'],d[0]['distance_in_meters'],d[0]['business_name'],d[0]['place_name'],d[0]['holding_number'],d[0]['road_name_number'],d[0]['super_sub_area'],d[0]['sub_area'],
                d[0]['popularity_ranking'],d[0]['pType'],d[0]['longitude'],d[0]['latitude'],d[0]['Address'],d[0]['area'],d[0]['city'],d[0]['subType'],d[0]['uCode']
                ,d[0]['postCode'],d[0]['unions'],d[0]['sub_district'],d[0]['district']
                ,d[0]['thana'],d[0]['ST_AsText(location)'],d[0]['ST_AsGeoJSON(bounds)'],d[0]['additional'],d[0]['images'],d[0]['data_source'],d[0]['bit'])

# postgres
def insert():
    try:
        # Connect to an existing database
        connection = psycopg2.connect(user="postgres",
                                      password="postgres",
                                      host="192.168.101.63",

                                      database="openstreetmap")

        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PostgreSQL details
        cursor.execute(
            "SELECT * FROM node_tags INNER JOIN nodes ON node_tags.node_id = nodes.node_id WHERE k='name' or k='name:bn'")
        row = cursor.fetchall()

        for l in row:
            print(l[3])
            print(l[4])
            print(l[5])
            putitem(l[3], int(l[4]), int(l[5]))


    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

# Querying 95 meter from the center point (36.879131, 10.243057)
def radiussearch():
    QueryRadiusInput={
            "FilterExpression": "area = :val1",
            "ExpressionAttributeValues": {
                ":val1": {"S": "Dhaka"},
            },

        }

    result= geoDataManager.queryRadius(
        dynamodbgeo.QueryRadiusRequest(
            dynamodbgeo.GeoPoint(23.8159601, 90.4362289), # center point
            90, sort = True))


    print(result)
def scantable():
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('data')

    resp = table.scan(ProjectionExpression="id")


    print(table.scan())

def get_item():
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('geo_test')

    resp = table.get_item(
        Key={
            'hashKey':  '668',
            'rangeKey': 'a3cb37b3-8f28-4c83-b455-3ae5220e94bc'
        }
    )

    if 'Item' in resp:
        print(resp['Item'])


radiussearch()
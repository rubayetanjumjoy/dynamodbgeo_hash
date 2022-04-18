def putitem(id,distance_in_meters,business_name,place_name,holding_number,road_name_number,sub_area,pType,Address,area,city,subType,postCode,unions, lat, lng):
    PutItemInput = {
        'Item': {
            'id': {'S': id},
            'distance_in_meters': {'S': distance_in_meters},

            'business_name': {'S': business_name},
            'place_name': {'S': place_name},
            'holding_number': {'S': holding_number},
            'road_name_number': {'S': road_name_number},
            'sub_area': {'S': sub_area},
            'pType': {'S': pType},
            'Address': {'S': Address},
            'area': {'S': area},
            'city': {'S': city},
            'subType': {'S': subType},
            'postCode': {'S': postCode},
            'unions': {'S': unions},

            'sub_district': {'S': sub_district},
            'district': {'S': district},
            'thana': {'S': thana},
            'popularity_ranking': {'S': popularity_ranking},
            'ST_AsText(location)': {'S': location},
            'ST_AsGeoJSON(bounds)': {'S': ST_AsGeoJSON(bounds)},
            'additional': {'S': additional},
            'images': {'S': images},
            'data_source': {'S': data_source},
            'bit': {'S': bit},


        },
        'ConditionExpression': "attribute_not_exists(hashKey)"
        # ... Anything else to pass through to `putItem`, eg ConditionExpression

    }
    geoDataManager.put_Point(dynamodbgeo.PutPointInput(
        dynamodbgeo.GeoPoint(lat, lng),  # latitude then latitude longitude
        str(uuid.uuid4()),  # Use this to ensure uniqueness of the hash/range pairs.
        PutItemInput  # pass the dict here
    ))
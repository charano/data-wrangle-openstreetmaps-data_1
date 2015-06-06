import pprint

def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def zip_pipeline():
    pipeline = [{"$match" : { "address.postcode" : {"$exists":1}}}
                ,{"$group" : {"_id" : "$address.postcode",
                             "count" : {"$sum" : 1}}}
                ,{"$sort" : {"count" : -1}}
                ]
    return pipeline

def city_pipeline():
    pipeline = [{"$match" : { "address.city" : {"$exists":1}}}
                ,{"$group" : {"_id" : "$address.city",
                             "count" : {"$sum" : 1}}}
                ,{"$sort" : {"count" : -1}}
                ]
    return pipeline

def school_pipeline():
    pipeline = [{"$match" : {"amenity" : {"$exists" : 1}, "amenity" : "school"}}
                ,{"$group" : {"_id" : "$name", "count" : {"$sum":1}}}
                ,{"$sort":{"count":-1}}
                ,{"$limit":10}
                ]
    return pipeline

def college_pipeline():
    pipeline = [{"$match" : {"amenity" : {"$exists" : 1}, "amenity" : "college"}}
                ,{"$group" : {"_id" : "$name", "count" : {"$sum":1}}}
                ,{"$sort":{"count":-1}}
                ,{"$limit":10}
                ]
    return pipeline

def university_pipeline():
    pipeline = [{"$match" : {"amenity" : {"$exists" : 1}, "amenity" : "university"}}
                ,{"$group" : {"_id" : "$name", "count" : {"$sum":1}}}
                ,{"$sort":{"count":-1}}
                ,{"$limit":10}
                ]
    return pipeline

def top_user_pipeline():
    pipeline = [{"$group" : {"_id" : "$created.user",
                             "count" : {"$sum" : 1}}}
                ,{"$sort" : {"count" : -1}}
                ,{"$limit":25}
                ]
    return pipeline

def one_time_user_pipeline():
    pipeline = [{"$group" : {"_id" : "$created.user",
                             "count" : {"$sum" : 1}}}
                ,{"$sort" : {"count" : -1}}
                ,{"$limit":1}
                ]
    return pipeline

def top_amenities_pipeline():
    pipeline = [{"$match" : {"amenity" : {"$exists" : 1}}}
                ,{"$group" : {"_id" : "$amenity", "count" : {"$sum":1}}}
                ,{"$sort":{"count":-1}}
                ,{"$limit":100}
                ]
    return pipeline

def top_religions_pipeline():
    pipeline = [{"$match" : {"amenity" : {"$exists" : 1}, "amenity" : "place_of_worship"}}
                ,{"$group" : {"_id" : "$religion", "count" : {"$sum":1}}}
                ,{"$sort":{"count":-1}}
                ,{"$limit":10}
                ]
    return pipeline

def top_cuisines_pipeline():
    pipeline = [{"$match" : {"amenity" : {"$exists" : 1}, "amenity" : "restaurant"}}
                ,{"$group" : {"_id" : "$cuisine", "count" : {"$sum":1}}}
                ,{"$sort":{"count":-1}}
                ,{"$limit":20}
                ]
    return pipeline

def marietta_cuisines_pipeline():
    pipeline = [{"$match" : {"amenity" : {"$exists" : 1}, "amenity" : "restaurant", "address.city" : "Marietta"}}
                ,{"$group" : {"_id" : "$cuisine", "count" : {"$sum":1}}}
                ,{"$sort":{"count":-1}}
                ,{"$limit":10}
                ]
    return pipeline

def decatur_cuisines_pipeline():
    pipeline = [{"$match" : {"amenity" : {"$exists" : 1}, "amenity" : "restaurant", "address.city" : "Decatur"}}
                ,{"$group" : {"_id" : "$cuisine", "count" : {"$sum":1}}}
                ,{"$sort":{"count":-1}}
                ,{"$limit":10}
                ]
    return pipeline

    
def timestamp_pipeline():
    pipeline = [{"$group" : {"_id" : {"year" : {"$year" : "$created.timestamp"}},
                             "count" : {"$sum" : 1}}}
                ,{"$sort" : {"count" : -1}}
                ]
    return pipeline

def aggregate(db, pipeline):
    result = db.cities.aggregate(pipeline)
    return result

if __name__ == '__main__':
    db = get_db('test')

    #Number of documents
    count_result = db.atlanta.find().count()
    print 'Number of documents'
    pprint.pprint(count_result)
    
    #Number of nodes
    nodes_result = db.atlanta.find({"type":"node"}).count()
    print 'Number of nodes'
    pprint.pprint(nodes_result)
    
    #Number of ways
    ways_result = db.atlanta.find({"type":"way"}).count()
    print 'Number of ways'
    pprint.pprint(ways_result)
    
    #Number of unique users
    users_result = len(db.atlanta.distinct("created.user"))
    print 'Number of unique users'
    pprint.pprint(users_result)
    
    #Top 1 contributing user
    top_user_pipeline = top_user_pipeline()
    top_user_result = db.atlanta.aggregate(top_user_pipeline)
    print 'Top contributor'
    pprint.pprint(top_user_result)
    
    #Zipcodes
    zip_pipeline = zip_pipeline()
    zip_result = db.atlanta.aggregate(zip_pipeline)
    print 'Zipcodes'
    pprint.pprint(zip_result)
    
    #Cities
    city_pipeline = city_pipeline()
    city_result = db.atlanta.aggregate(city_pipeline)
    print 'Cities'
    pprint.pprint(city_result)
    
    #Top amenities
    top_amenities_pipeline = top_amenities_pipeline()
    amenity_result = db.atlanta.aggregate(top_amenities_pipeline)
    print 'Amenities'
    pprint.pprint(amenity_result)
    
    #Top religions
    top_religions_pipeline = top_religions_pipeline()
    top_religions_result = db.atlanta.aggregate(top_religions_pipeline)
    print 'Top Religions'
    pprint.pprint(top_religions_result)
    
    #Top cuisines
    top_cuisines_pipeline = top_cuisines_pipeline()
    top_cuisines_result = db.atlanta.aggregate(top_cuisines_pipeline)
    print 'Top Cuisines'
    pprint.pprint(top_cuisines_result)
    
    #Marietta cuisines
    marietta_cuisines_pipeline = marietta_cuisines_pipeline()
    marietta_cuisines_result = db.atlanta.aggregate(marietta_cuisines_pipeline)
    print 'Marietta Cuisines'
    pprint.pprint(marietta_cuisines_result)
    
    #Decatur cuisines
    decatur_cuisines_pipeline = decatur_cuisines_pipeline()
    decatur_cuisines_result = db.atlanta.aggregate(decatur_cuisines_pipeline)
    print 'Decatur Cuisines'
    pprint.pprint(decatur_cuisines_result)
    
    #Schools
    school_pipeline = school_pipeline()
    school_result = db.atlanta.aggregate(school_pipeline)
    print 'Schools'
    pprint.pprint(school_result)
    
    #Colleges
    college_pipeline = college_pipeline()
    college_result = db.atlanta.aggregate(college_pipeline)
    print 'Colleges'
    pprint.pprint(college_result)
    
    #Universities
    university_pipeline = university_pipeline()
    university_result = db.atlanta.aggregate(university_pipeline)
    print 'Universities'
    pprint.pprint(university_result)

    #Number of records created every year
    timestamp_pipeline = timestamp_pipeline()
    timestamp_result = db.atlanta.aggregate(timestamp_pipeline)
    print 'Number of records created/year'
    pprint.pprint(timestamp_result)
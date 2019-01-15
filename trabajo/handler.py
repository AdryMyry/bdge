import pymongo
from pymongo import MongoClient
from bson.code import Code
from bson.json_util import dumps

import timeit

MONGO_SERVER = "ec2-3-82-61-111.compute-1.amazonaws.com"
MONGO_PORT = 27017

def make_response(data, time):
    body = {
        "data": data,
        "elapsedTime": time,
    }

    response = {
        "statusCode": 200,
        "body": dumps(body)
    }

    return response

def rq1_agg_handler(event, context):

    start_time = timeit.default_timer()

    client = MongoClient(MONGO_SERVER,MONGO_PORT)
    db = client.stackoverflow

    dataRQ1 = db.users.aggregate( [
            {'$lookup': {
                'from': 'posts',
                'localField' : 'Id',
                'foreignField' : 'OwnerUserId',
                'as': 'posts'}
            },
            {'$project' : {
                'Id' : True,
                'posts': {
                    '$filter' : {
                    'input' : '$posts',
                    'as' : 'post',
                    'cond' : { '$eq': ['$$post.PostTypeId', 1] }
                }}
            }},
            {'$project' : {
                'Id' : True,
                'postsCount': { '$size' : '$posts'}
            }},
            {'$group' : {
                '_id' : '$postsCount',
                'usersCount': { '$sum' : 1 }
            }},
            {'$sort' :  { '_id' : -1 } }
    ])
    
    dataRQ1 = list(dataRQ1)

    elapsed = timeit.default_timer() - start_time
    return make_response(dataRQ1, elapsed)


def rq1_mapred_handler(event, context):

    start_time = timeit.default_timer()

    client = MongoClient(MONGO_SERVER,MONGO_PORT)
    db = client.stackoverflow

    rq1_map = Code(
    """
    function () {
        if (this.PostTypeId == 1) {
            emit(this.OwnerUserId, 1);
        }
    }
    """)

    rq1_reduce = Code(
    '''
    function (key, values)
    {
        return Array.sum(values);
    }
    ''')

    db.posts.map_reduce(rq1_map, rq1_reduce, out='rq1')

    rq1_map2 = Code(
    """
    function () {
        emit(this.Id, 0);
    }
    """)

    rq1_reduce2 = Code("""
    function (key, values) {
        return Array.sum(values);
    }
    """)

    db.users.map_reduce(rq1_map2, rq1_reduce2, out={'reduce' : 'rq1'})

    rq1MR = list(db.rq1.find())

    elapsed = timeit.default_timer() - start_time
    return make_response(rq1MR, elapsed)

def rq2_agg_handler(event, context):

    start_time = timeit.default_timer()

    client = MongoClient(MONGO_SERVER,MONGO_PORT)
    db = client.stackoverflow

    dataRQ2 = db.users.aggregate( [
            {'$lookup': {
                'from': 'posts',
                'localField' : 'Id',
                'foreignField' : 'OwnerUserId',
                'as': 'posts'}
            },
            {'$project' : {
                'Id' : True,
                'answers': {
                    '$filter' : {
                    'input' : '$posts',
                    'as' : 'post',
                    'cond' : { '$eq': ['$$post.PostTypeId', 2] }
                }}
            }},
            {'$project' : {
                'Id' : True,
                'ansCount': { '$size' : '$answers'}
            }},
            {'$group' : {
                '_id' : '$ansCount',
                'usersCount': { '$sum' : 1 }
            }},
            {'$sort' :  { '_id' : -1 } }
    ])

    data = list(dataRQ2)

    elapsed = timeit.default_timer() - start_time
    return make_response(data, elapsed)

def rq2_mapred_handler(event, context):

    start_time = timeit.default_timer()

    client = MongoClient(MONGO_SERVER,MONGO_PORT)
    db = client.stackoverflow
    
    rq2_map = Code(
    """
    function () {
        if (this.PostTypeId == 2) {
            emit(this.OwnerUserId, 1);
        }
    }
    """)

    rq2_reduce = Code(
    '''
    function (key, values)
    {
        return Array.sum(values);
    }
    ''')
    db.posts.map_reduce(rq2_map, rq2_reduce, out='rq2')

    rq2_map2 = Code(
    """
    function () {
        emit(this.Id, 0);
    }
    """)

    rq2_reduce2 = Code("""
    function (key, values) {
        return Array.sum(values);
    }
    """)

    db.users.map_reduce(rq2_map2, rq2_reduce2, out={'reduce' : 'rq2'})
    
    data = list(db.rq2.find())

    elapsed = timeit.default_timer() - start_time
    return make_response(data, elapsed)

def rq3_agg_handler(event, context):

    start_time = timeit.default_timer()

    client = MongoClient(MONGO_SERVER,MONGO_PORT)
    db = client.stackoverflow

    dataRQ3 = db.users.aggregate( [
            {'$lookup': {
                'from': 'posts',
                'localField' : 'Id',
                'foreignField' : 'OwnerUserId',
                'as': 'posts'}
            },
            {'$project' : {
                'posts': {
                    '$filter' : {
                    'input' : '$posts',
                    'as' : 'post',
                    'cond' : { '$eq': ['$$post.PostTypeId', 1] }
                }},
                'answers': {
                    '$filter' : {
                    'input' : '$posts',
                    'as' : 'post',
                    'cond' : { '$eq': ['$$post.PostTypeId', 2] }
                }}
            }},
            {'$project' : {
                'postsCount': { '$size' : '$posts'},
                'ansCount': { '$size' : '$answers'}

            }}
    ])
    
    data = list(dataRQ3)

    elapsed = timeit.default_timer() - start_time
    return make_response(data, elapsed)

def rq3_mapred_handler(event, context):

    start_time = timeit.default_timer()

    client = MongoClient(MONGO_SERVER,MONGO_PORT)
    db = client.stackoverflow

    rq3_map = Code(
    """
    function () {
        object = {
            questions: 0,
            answers: 0
        };    
        if (this.PostTypeId == 2) {
            object.answers = 1;
            object.questions = 0;
        } else if (this.PostTypeId == 1) {
            object.questions = 1;
            object.answers = 0;
        }
        emit(this.OwnerUserId, object);
    }
    """)

    rq3_reduce = Code(
    '''
    function (key, values)
    {
        questions = 0;
        answers = 0;
        for(i = 0; i<values.length; i++) {
            questions += values[i].questions;
            answers += values[i].answers;
        }
        return {questions, answers};
    }
    ''')

    db.posts.map_reduce(rq3_map, rq3_reduce, out='rq3')

    rq3_map2 = Code(
    """
    function () {
        object = {
            questions: 0,
            answers: 0
        };
        emit(this.Id, object);
    }
    """)

    rq3_reduce2 = Code("""
    function (key, values)
    {
        questions = 0;
        answers = 0;
        for(i = 0; i<values.length; i++) {
            questions += values[i].questions;
            answers += values[i].answers;
        }
        return {questions, answers};
    }
    """)

    db.users.map_reduce(rq3_map2, rq3_reduce2, out={'reduce' : 'rq3'})
    
    data = list(db.rq3.find())
    elapsed = timeit.default_timer() - start_time
    return make_response(data, elapsed)

def rq4_agg_handler(event, context):

    start_time = timeit.default_timer()

    client = MongoClient(MONGO_SERVER,MONGO_PORT)
    db = client.stackoverflow
    
    rq4 = db.posts.aggregate( [
        {'$match': { 'AcceptedAnswerId' : {'$ne': ''}}},
        {'$lookup': {
            'from': "posts", 
            'localField': "AcceptedAnswerId",
            'foreignField': "Id",
            'as': "answer"}
        },
        { 
            '$unwind' : '$answer'
        },
        {
            '$project' : { 'OwnerUserId': True, 
                           'answerer' : '$answer.OwnerUserId'
                         }
        },
        {
            '$group' : {'_id' : {'min' : { '$min' : ['$OwnerUserId' , '$answerer'] },
                                 'max' : { '$max' : ['$OwnerUserId' , '$answerer'] }},
                        'answers' : {'$addToSet' : { '0q':'$OwnerUserId', '1a': '$answerer'}}
                        }
        },
        {
            '$project': {
                'answers' : True,
                'nanswers' : { '$size' : '$answers'}
            }
        },
        {
            '$match' : { 'nanswers' : { '$eq' : 2}}
        }
    ])
    data = list(rq4)

    elapsed = timeit.default_timer() - start_time
    return make_response(data, elapsed)

# def schema(event, context):

#     data = ...
#     return make_response(data)
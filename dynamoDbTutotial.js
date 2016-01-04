/*
 * Create and get information about tables
 */

//Creates the Music table
var params = {
    TableName: 'Music',
    KeySchema: [
        {
            AttributeName: 'Artist',
            KeyType: 'HASH'
        },
        {
            AttributeName: 'SongTitle',
            KeyType: 'RANGE'
        }
    ],
    AttributeDefinitions: [
        {
            AttributeName: 'Artist', AttributeType: 'S'
        },
        {
            AttributeName: 'SongTitle', AttributeType: 'S'
        }
    ],
    ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 2
    }
};

dynamodb.createTable(params, function(err, data){
   if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
   } 
   
   console.log(JSON.stringify(data, null, 2));
   
});

//Retreive information about the table
params = {
  TableName: 'Music'  
};

dynamodb.describeTable(params, function(err, data){
   if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
   } 
   
   console.log(JSON.stringify(data, null, 2));
});

/*
 * Inserting Items
 */

//Write a single item
params = {
    TableName: 'Music',
    Item: {
        'Artist': 'No one you know',
        'SongTitle': 'Call me today',
        'AlbumTitle': 'Somewhat famous',
        'Year': 2015,
        'Price': 2.14,
        'Genre': 'Country',
        'Tags': {
            'Composers': [
                'Smith',
                'Jones',
                'Davis'
            ],
            'LengthInSeconds': 214
        }
    }
};

dynamodb.putItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
   } 
   
   console.log(JSON.stringify(data, null, 2));
});

//Write a single item with a condition check to avoid overwriting an already existing item
params = {
    TableName: 'Music',
    Item: {
        'Artist': 'No one you know',
        'SongTitle': 'Call me today - Remix',
        'AlbumTitle': 'Somewhat famous',
        'Year': 2015,
        'Price': 2.14,
        'Genre': 'Country',
        'Tags': {
            'Composers': [
                'Smith',
                'Jones',
                'Davis'
            ],
            'LengthInSeconds': 214
        }
    },
    'ConditionExpression': 'attribute_not_exists(Artist) and attribute_not_exists(SongTitle)'
};

dynamodb.putItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
   } 
   
   console.log(JSON.stringify(data, null, 2));
});

//Write multiple items
params = {
  RequestItems:{
        'Music': [
            {
                PutRequest: {
                    Item: {
                        "Artist": "No One You Know",
                        "SongTitle": "My Dog Spot",
                        "AlbumTitle":"Hey Now",
                        "Price": 1.98,
                        "Genre": "Country",
                        "CriticRating": 8.4                        
                    }
                }
            },
            { 
                PutRequest: {
                    Item: {
                        "Artist": "The Acme Band",
                        "SongTitle": "Still In Love",
                        "AlbumTitle":"The Buck Starts Here",
                        "Price": 2.47,
                        "Genre": "Rock",
                        "PromotionInfo": {
                            "RadioStationsPlaying":[
                                "KHCR", "KBQX", "WTNR", "WJJH"
                            ],
                            "TourDates": {
                                "Seattle": "20150625",
                                "Cleveland": "20150630"
                            },
                            "Rotation": "Heavy"
                        }
                    }
                }
            }, 
            { 
                PutRequest: {
                    Item: {
                        "Artist": "The Acme Band",
                        "SongTitle": "Look Out, World",
                        "AlbumTitle":"The Buck Starts Here",
                        "Price": 0.99,
                        "Genre": "Rock"
                    }
                }
            }            
        ]
    }  
};

dynamodb.batchWriteItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
   } 
   
   console.log(JSON.stringify(data, null, 2));
});

/*
 * Retreiving Items
 */

//Read an Item Using GetItem
params = {
    TableName: 'Music',
    Key: {
        'Artist': 'No one you know',
        'SongTitle': 'Call me today'
    }
};

dynamodb.getItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
   } 
   
   console.log(JSON.stringify(data, null, 2));
});

//Retrieve a subset of attributes using a projection expression
params = {
    TableName: 'Music',
    Key: {
        'Artist': 'No one you know',
        'SongTitle': 'Call me today'
    },
    ProjectionExpression: 'AlbumTitle, Tags'
};

dynamodb.getItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
   } 
   
   console.log(JSON.stringify(data, null, 2));
});

//Handling attribute names that are also reserved words
params = {
    TableName: 'Music',
    Key: {
        'Artist': 'No one you know',
        'SongTitle': 'Call me today'
    },
    ProjectionExpression: 'AlbumTitle, #y',
    ExpressionAttributeNames: {'#y': 'Year'}
};

dynamodb.getItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
   } 
   
   console.log(JSON.stringify(data, null, 2));
});

//Retreiving nested attributes using document path notation
params = {
    TableName: 'Music',
    Key: {
        'Artist': 'No one you know',
        'SongTitle': 'Call me today'
    },
    ProjectionExpression: 'AlbumTitle, #y, Tags.Composers[0], Tags.LengthInSeconds',
    ExpressionAttributeNames: {'#y': 'Year'}
};

dynamodb.getItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
   } 
   
   console.log(JSON.stringify(data, null, 2));
});

//Retreiving multiple items using BatchGetItem
params = {
    RequestItems: {
        'Music': {
            Keys: [
                {
                    "Artist": "No One You Know",
                    "SongTitle": "My Dog Spot"
                },
                {
                    "Artist": "No one you know",
                    "SongTitle": "Call me today"
                },
                {
                    "Artist": "The Acme Band",
                    "SongTitle": "Still In Love"
                },
                {
                    "Artist": "The Acme Band",
                    "SongTitle": "Look Out, World"
                }
            ],
            ProjectionExpression: 'PromotionInfo, CriticRating, Price, Tags.LengthInSeconds'
        }      
    }
};

dynamodb.batchGetItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));    
});

/*
 * Running queries
 */

//Query using a hash key attribute
params = {
    TableName: 'Music',
    KeyConditionExpression: 'Artist = :artist',
    ExpressionAttributeValues: {
        ':artist': 'No One You Know'
    }
};

dynamodb.query(params, function(err, data) {
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));    
});

//Query using hash and range key attributes
params = {
    TableName: 'Music',
    ProjectionExpression: 'SongTitle',
    KeyConditionExpression: 'Artist = :artist and begins_with(SongTitle, :letter)',
    ExpressionAttributeValues: {
        ':artist': 'The Acme Band',
        ':letter': 'S'
    }
};

dynamodb.query(params, function(err, data) {
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));    
});

//Filter query result
params = {
    TableName: 'Music',
    ProjectionExpression: 'SongTitle, PromotionInfo.Rotation',
    KeyConditionExpression: 'Artist = :artist',
    FilterExpression: 'size(PromotionInfo.RadioStationsPlaying) >= :howmany',
    ExpressionAttributeValues: {
        ':artist': 'The Acme Band',
        ':howmany': 3
    }
};

dynamodb.query(params, function(err, data) {
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));    
});

//Scan the table
params = {
    TableName: 'Music'  
};

dynamodb.scan(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));        
});

//Create a global secondary index
params = {
    TableName: 'Music',
    AttributeDefinitions: [
        {
            AttributeName: 'Genre', AttributeType: 'S'
        },
        {
            AttributeName: 'Price', AttributeType: 'N'
        }
    ],
    GlobalSecondaryIndexUpdates: [
        {
            Create: {
                IndexName: 'GenreAndPriceIndex',
                KeySchema: [
                    {
                        AttributeName: 'Genre',
                        KeyType: 'HASH'
                    },
                    {
                        AttributeName: 'Price',
                        KeyType: 'RANGE'
                    }
                ],
                Projection: {
                    'ProjectionType': 'ALL'
                },
                ProvisionedThroughput: {
                    "ReadCapacityUnits": 1,"WriteCapacityUnits": 1
                }
            }   
        }
    ]
};

dynamodb.updateTable(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));     
});

//Query the index 
params = {
    TableName: 'Music',
    IndexName: 'GenreAndPriceIndex',
    KeyConditionExpression: 'Genre = :genre and Price > :price',
    ExpressionAttributeValues: {
        ':genre': 'Country',
        ':price': 2.00
    },
    ProjectionExpression: 'SongTitle, Price'
};

dynamodb.query(params, function(err, data) {
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));    
});

//Scan the index
params = {
    TableName: 'Music',
    IndexName: 'GenreAndPriceIndex',
    ProjectionExpression: 'Genre, Price, SongTitle, Artist, AlbumTitle'
};

dynamodb.scan(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));   
});

//(Bonus) - Find which song doesn't have the price attribute
params = {
    TableName: 'Music',
    Item: {
        'Artist': 'No one you know',
        'SongTitle': 'My Dog Spot - Remix',
        'AlbumTitle': 'Somewhat famous',
        'Year': 2015,
        'Genre': 'Country',
        'Tags': {
            'Composers': [
                'Smith',
                'Jones',
                'Davis'
            ],
            'LengthInSeconds': 214
        }
    },
    'ConditionExpression': 'attribute_not_exists(Artist) and attribute_not_exists(SongTitle)'
};

dynamodb.putItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
   } 
   
   console.log(JSON.stringify(data, null, 2));
});

params = {
    TableName: 'Music',
    FilterExpression: 'attribute_not_exists (Price)'
};

dynamodb.scan(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));   
});

/*
 * Updating Items
 */

//Update an item
params = {
    TableName: 'Music',
    Key: {
        'Artist': 'No One You Know',
        'SongTitle': 'Call Me Today'
    },
    //UpdateExpression: 'SET RecordLabel = :label',
    UpdateExpression: 'SET Price = :price',
    ExpressionAttributeValues: {
        ':price': 0.99 
    },
    //ReturnValues: 'ALL_NEW'
    ReturnValues: 'ALL_NEW'
};

dynamodb.updateItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));    
});

//Specify a conditional write
params = {
    TableName: 'Music',
    Key: {
        'Artist': 'No One You Know',
        'SongTitle': 'Call Me Today'
    },
    UpdateExpression: 'SET RecordLabel = :label',
    ExpressionAttributeValues: {
        ':label': 'New Wave Recording, Inc.' 
    },
    ConditionExpression: 'attribute_not_exists(RecordLabel)',
    ReturnValues: 'ALL_NEW'
};

dynamodb.updateItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));    
});

//Specify an Atomic Counter
params = {
    TableName: "Music",
    Key: {
        "Artist":"No One You Know",
        "SongTitle":"Call Me Today"
    },
    UpdateExpression: "SET Plays = :val",
    ExpressionAttributeValues: { 
        ":val": 0
    },
    ReturnValues: "UPDATED_NEW"
};

dynamodb.updateItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));    
});

params = {
    TableName: "Music",
    Key: {
        "Artist":"No One You Know",
        "SongTitle":"Call Me Today"
    },
    UpdateExpression: "SET Plays = Plays + :incr",
    ExpressionAttributeValues: { 
        ":incr": 1
    },
    ReturnValues: "UPDATED_NEW"
};

dynamodb.updateItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));    
});

/*
 * Deleting Items
 */

//Delete an item
params = {
    TableName: 'Music',
    Key: {
        Artist: "The Acme Band", 
        SongTitle: "Look Out, World"
    }
};

dynamodb.deleteItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));  
});

//specify a conditional delete
params = {
    TableName: 'Music',
    Key: {
        Artist: "No One You Know", 
        SongTitle: "My Dog Spot"
    },
    ConditionExpression: "Price = :price",
    ExpressionAttributeValues: {
        ':price': 0.00
    }
};

dynamodb.deleteItem(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));  
});

/*
 * Deleting a whole table
 */

//Delete the table
params = {
    TableName: 'Music'  
};

dynamodb.deleteTable(params, function(err, data){
    if(err) {
        console.log(JSON.stringify(err, null, 2));
        return;
    } 
   
   console.log(JSON.stringify(data, null, 2));  
});

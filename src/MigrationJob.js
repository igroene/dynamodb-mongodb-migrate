'use strict';
const lodash = require('lodash');
const RateLimiter = require('limiter').RateLimiter;
const DynamoDBDAO = require('./dao/DynamoDBDAO');
const MongoDBDAO = require('./dao/MongoDBDAO');


class MigrationJob {
    constructor(sourceTableName, targetTableName, targetDbName,sourceConnectionOptions, targetConnectionOptions, dynamodbEvalLimit, dynamoDbReadThroughput) {
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.targetDbName = targetDbName;
        this.mapperFunction = (item) => { return item; };
        this.filterFunction = () => { return true; };
        this.dynamoDBDAO = new DynamoDBDAO(sourceTableName,sourceConnectionOptions.region,sourceConnectionOptions.accessKeyId,sourceConnectionOptions.secretAccessKey);
        this.mongoDBDAO = new MongoDBDAO(this.targetTableName, this.targetDbName,targetConnectionOptions.host, targetConnectionOptions.user, targetConnectionOptions.password);
        this.dynamodbEvalLimit = dynamodbEvalLimit || 100;
        this.filterExpression = null;
        this.expressionAttributeNames = null;
        this.expressionAttributeValues = null;
	this.segment = null;
	this.totalSegments = null;
	this.lastEvalKey = null;
        this.dynamoDbReadThroughput = dynamoDbReadThroughput ? Number(dynamoDbReadThroughput) : 25;
        this.limiter = new RateLimiter(this.dynamoDbReadThroughput, 1000);
        this._removeTokens = (tokenCount) => {
            return new Promise((resolve, reject) => {
                this.limiter.removeTokens(tokenCount, () => {
                    resolve();
                });
            });
        }
    }

    setMapperFunction(mapperFunction) {
        this.mapperFunction = mapperFunction
    }

    setFilterFunction(filterFunction) {
        this.filterFunction = filterFunction;
    }

    setSourcefilterExpression(filterExpression, expressionAttributeNames, expressionAttributeValues) {
        this.filterExpression = filterExpression;
        this.expressionAttributeNames = expressionAttributeNames;
        this.expressionAttributeValues = expressionAttributeValues;
    }

   setParallelism(segmentNum, totalSegments) {
	this.segment = segmentNum;
	this.totalSegments = totalSegments;
   }

   setStartingKey(key) {
       this.lastEvalKey = key;
   }

    run() {
        let ctx = this;
        return new Promise(async (resolve, reject) => {
            try {
                let lastEvalKey, startTime, endTime, totalItemCount = 0, iteration = 1, permitsToConsume = 1;
                lastEvalKey = JSON.parse(this.lastEvalKey);
                do {
                    startTime = new Date().getTime();
                    await ctx._removeTokens(permitsToConsume);
                    let sourceItemResponse = await ctx.dynamoDBDAO.scan(ctx.filterExpression, ctx.expressionAttributeNames, ctx.expressionAttributeValues, lastEvalKey, ctx.dynamodbEvalLimit, ctx.segment, ctx.totalSegments);
                    totalItemCount += sourceItemResponse.Count;
                    let consumedCapacity = sourceItemResponse.ConsumedCapacity.CapacityUnits;
                    //console.log('Consumed capacity ', consumedCapacity);
                    console.log('Received ', sourceItemResponse.Count, ' items at iteration ', iteration, ' and total of ', totalItemCount, ' items received');
                    permitsToConsume = Math.round(consumedCapacity - 1);
                    if (permitsToConsume < 1) {
                        permitsToConsume = 1;
                    }
                    let sourceItems = sourceItemResponse && sourceItemResponse.Items ? sourceItemResponse.Items : [];
                    let targetItems = lodash
                        .chain(sourceItems)
                        .filter(ctx.filterFunction)
                        .map(ctx.mapperFunction)
                        .value();
                    if (targetItems.length > 0) {
			try {
                            let results = await ctx.mongoDBDAO.intertOrUpdateItems(targetItems);
                            console.log('Inserted mongodb doc count : ', results.insertedCount);
                    	}
		        catch(e) {
                            if ('result' in e) {
                                console.error(e.result.result.writeErrors);
                                console.log('Inserted mongodb doc count : ', e.result.result.nInserted);
                            }
                            else {
                                console.error(e);
                            }
		        }
		    }
                    if (sourceItemResponse && sourceItemResponse.LastEvaluatedKey) {
                        lastEvalKey = sourceItemResponse.LastEvaluatedKey;
                    } else {
                        lastEvalKey = null;
                    }
                    endTime = new Date().getTime();
                    console.log('Loop completion time : ', endTime - startTime, ' ms - ', 'LastEvalKey: ', lastEvalKey);
                    iteration++;
                } while (lastEvalKey);
                console.log('Migration completed');
                resolve();
            } catch (error) {
                console.error(error);
                reject(error);
            }
        });
    }
}

module.exports = MigrationJob;

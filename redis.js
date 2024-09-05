"use strict"
const appconfig = require('./settings');
const postTimeKey = "VOLVO_LAST_DATA_PROCESS_TIME";

let client;

exports.client = client

const redis = require('redis');
const fakeRedis = require('redis-mock');
const bluebird = require('bluebird');

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(fakeRedis.RedisClient.prototype);


// if (process.env.NODE_ENV === 'test') {
//     appconfig.printlog(null, "using mocked redis for tests");
//     redisClient = fakeRedis.createClient();
// } else {
    // appconfig.printlog(null, "create redis client from ",   process.env.REDIS_HOST + " port " +   process.env.REDIS_PORT)
    // if (process.env.REDIS_PORT === 6380) {
        // REDIS SSL CONNECTION
   const  redisClient = redis.createClient(
            '6380',
            'msgiot-redis01-dev.redis.cache.windows.net',
            {
                auth_pass: 'LHw7DS2YkOK788uvaNXdbCwapJnWKWu39PrKYLV+7E4=',
                tls: { servername: 'msgiot-redis01-dev.redis.cache.windows.net' }
            });
    // }
    // else{
    //     redisClient = redis.createClient({
    //         host: process.env.REDIS_HOST,
    //         port: process.env.REDIS_PORT,
    //         password: process.env.REDIS_PASSWORD
    //     });
    // }
    // appconfig.printlog(context, "redisClient", redisClient)
// }

// azure functions and aws lambda functions sometimes preserve global variables if the function is run again without being torn down
// this cache is just a memory cache that might get used and save some time in this case
// otherwise redis is used

// todo: change to a map as needed, depending on performance tests
// see this benchmark https://jsperf.com/es6-map-vs-object-properties/73
// in my browser, object set is 2x faster than map set, but object get is is 20x slower than map get
// maps have had many optimizations in 2018 versions of v8 so this may not be the case on azure
let cache = {};

// set (or update) local variable and redis
// key is a string.  normally will look like "gateway:ABCDEFGH" where "gateway:" is distinguishing the type of cache entry
// value is an object.  will be serialized with json stringify
// expiration is time in seconds before the cache entry should be discarded
module.exports.storeCache = async function checkCache(context, key, value, expiration) {
    // appconfig.printlog(context, "cache: setting key " + key);
    cache[key] = { expiresAt: (new Date()).getTime() + (expiration * 1000), value: JSON.stringify(value) };
    try {
        await redisClient.setAsync(key, JSON.stringify(value), 'EX', expiration); // todo: remove 'await'.  we won't read back from redis as long as this instance is still running, so all that matters is that it gets written out before exiting overall
        // 'await' is needed with some tests using redis-mock.  not sure if it's needed with real redis
    } catch (error) {
        appconfig.printlog(context, "", error);
    }
    return;
}

// check local variable and redis.  return undefined if not found, otherwise returns the json object
module.exports.loadCache = async function checkCache(context, key, expiration) {

    // appconfig.printlog(context, "inside cache.js process.env", process.env)
    // appconfig.printlog(context, "inside loadCache redisClient.connection_options", redisClient.connection_options)
    // if (process.env.REDIS_PORT=== "6380" && redisClient.connection_options.port === 6379) {
        // redisClient = redis.createClient(
        //     process.env.REDIS_PORT,
        //     process.env.REDIS_HOST,
        //     {
        //         auth_pass: process.env.REDIS_PASSWORD,
        //         tls: { servername: process.env.REDIS_HOST }
        //     });
        // appconfig.printlog(context, "loadCache redis client", redisClient)
    // }
    try {
    // appconfig.printlog(context, "loadCache cache["+ key + "]=", cache[key]);
    // appconfig.printlog(context, "loadCache expiration", expiration)
    } catch (e1) {
        appconfig.printlog(context, "exception when print cache[key]", e1)
    }
    if (cache[key] !== undefined) {
        try {
        if (cache[key].expiresAt > (new Date()).getTime()) {
            // appconfig.printlog(context, "cahce["+key+"]=", cache[key].value)
            return JSON.parse(cache[key].value);
        } else {
            // key expired, will also be expired in redis because they have the same expiration time
            delete cache[key];
            return null; // match redis return value for missing key
        }
        } catch (e2) {
            appconfig.printlog(context, "Exception when cache[key] has value", e1)
        }
    } else {
        // check redis - our memory cache may be empty because the function did not persist
        try {
            // appconfig.printlog(context, " cache[key] undefined for redisClient.connection", redisClient)
            // appconfig.printlog(context, " cache[key] undefined for redisClient.connection", redisClient.connection_options.port + " " + 
            //     redisClient.connection_options.host)
            const redisResult = await redisClient.getAsync(key);
            // appconfig.printlog(context, "redisResult for key " + key, redisResult); 
      
            if (redisResult !== null) {
                const value = JSON.parse(redisResult);
                cache[key] = { expiresAt: (new Date()).getTime() + (expiration * 1000), value: JSON.stringify(value) };
                return value;
            } else {
                return redisResult;
            }
        } catch (e) {
            appconfig.printlog(context, "exceptoin when getDatabase when cache[key] undefined trying to call redisClient.getAsync(key)", e);
            return redisResult;
        }
    }
}

// testing only
module.exports.clearMemoryCache = function clearMemoryCache() {
    cache = {};
}

function np(fn) {
    return new Promise((ok, fail) => fn((err, res) => err ? fail(err) : ok(res)));
}

exports.get = async function get(key) {
const res = await np(cb => redisClient.get(key, cb));
try {
    return JSON.parse(res);
} catch (e) {
    return res;
}
}

exports.set = async function set(key, value) {
const stringified = typeof value === 'object' ? JSON.stringify(value) : value;
return await np(cb => redisClient.set(key, stringified, cb));
}

//clearing a key from redis
module.exports.deleteKey = async function deleteKey(key) {
    return await np(cb => redisClient.del(key, cb));
  }
//exports.createClient = async function() {
if (process.env.NODE_ENV === 'test') {
  if (infoLogging === true || infoLogging === 'true' || infoLogging === 1) {
    // appconfig.printlog(context, 'Using fake redis');
  }
  client = fakeRedis.createClient();
} else {
  if (appconfig.config.redis_port === '6380') {
    // REDIS SSL CONNECTION
    client = redis.createClient(
      appconfig.config.redis_port,
      appconfig.config.redis_host,
      {
        auth_pass: appconfig.config.redis_password,
        tls: { servername: appconfig.config.redis_host }
      });
  }
  else {
    client = redis.createClient({
      host: appconfig.config.redis_host,
      port: appconfig.config.redis_port,
      password: appconfig.config.redis_password
    });
  }
}
//return client;
//}

exports.get = async function get(key) {
  const res = await np(cb => client.get(key, cb));
  try {
    return JSON.parse(res);
  } catch (e) {
    return res;
  }
}

exports.set = async function set(key, value) {
  const stringified = typeof value === 'object' ? JSON.stringify(value) : value;
  return await np(cb => client.set(key, stringified, cb));
}

exports.multi = async function multi(keys) {
  const m = client.multi();

  for (const key of keys) {
    m.get(key);
  }

  const res = await np(cb => m.exec(cb));

  try {
    return res.map(n => {
      try {
        return JSON.parse(n);
      } catch (e) {
        return n;
      }
    });

  } catch (e) {
    return res;
  }
}

exports.setAdd = function setAdd(key, value) {
  let values = value.map ? value.filter(n => typeof n !== 'undefined') : [value];
  const stringified = values.map(n => JSON.stringify(n));
  return np(cb => client.sadd(key, stringified, cb));
}

exports.getSetMembers = async function getSetMembers(key) {
  const res = await np(cb => client.smembers(key, cb));
  try {
    return res.map(n => {
      try {
        return JSON.parse(n);
      } catch (e) {
        return n;
      }
    });
  } catch (e) {
    return res;
  }
}

// For unit testing
exports.clear = async function clear() {
  const res = await np(cb => client.flushall(cb));
  try {
    return JSON.parse(res);
  } catch (e) {
    return res;
  }
}

//clearing a key from redis
exports.deleteKey = async function deleteKey(key) {
  return await np(cb => client.del(key, cb));
}

exports.setRedisMap = async function setRedisMap(context, key, myMap) {
  let mapObjEntries = myMap.entries();
  let mapObjStr = JSON.stringify(Array.from(mapObjEntries));
  await this.set(key, mapObjStr);

  // veriy the result
  let mapObjFromRedis = await this.get(key);
  let mapFromRedis = new Map(mapObjFromRedis);
  // appconfig.printlog(context, "mapFromRedis saved for key " + key, mapFromRedis);
}

exports.getRedisMap = async function getRedisMap(context, key) {
  let mapObjFromRedis = await this.get(key);
  let mapFromRedis; 
  if (mapObjFromRedis === undefined) {
    mapFromRedis = new Map(); 
  } else {
    mapFromRedis = new Map(mapObjFromRedis);
  }
  // appconfig.printlog(context, "mapFromRedis for " + key, mapFromRedis);
  return mapFromRedis; 
}

exports.getLastPostTime = async function getLastPostTime(context) {
  let lastPostTime = await this.get(postTimeKey);
  if(!lastPostTime) {
    lastPostTime =   new Date(new Date().getTime() - 3600000);
  } 
  return lastPostTime; 
}
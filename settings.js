const { Pool } = require("pg");
//const Promise = require("Promise");
const Registry = require("azure-iothub").Registry;
// require('dotenv').config();
 
const config = {
  database: process.env.DATABASE,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
  ssl:process.env.DB_SSL,
  redis_host : process.env.REDIS_HOST,
  redis_password : process.env.REDIS_PASSWORD,
  redis_port : process.env.REDIS_PORT,
  blob_storage_con_string: process.env.BLOB_STORAGE_CON_STRING,
  // container_name: process.env.CONTAINER_NAME,
  // blob_storag_url: process.env.BLOB_STORAGE_URL,
  service_bbus_con_string: process.env.SERVICE_BUS_CON_STRING,
  volvo_token_api_url: process.env.VOLVO_TOKEN_API_URL,
  volvo_token_api_client_id: process.env.VOLVO_TOKEN_API_CLIENT_ID,
  volvo_token_api_client_secret: process.env.VOLVO_TOKEN_API_CLIENT_SECRET,
  volvo_token_api_scope: process.env.VOLVO_TOKEN_API_SCOPE, 
  volvo_token_api_cookie: process.env.VOLVO_TOKEN_API_COOKIE,
  volvo_api_url: process.env.VOLVO_API_URL,
  volvo_api_client_id: process.env.VOLVO_API_CLIENT_ID,
  volvo_api_client_secret: process.env.VOLVO_API_CLIENT_SECRET
  // group_size : process.env.GROUP_SIZE,
  // batch_size : process.env.BATCH_SIZE,
};

async function getConfig() {
  return config;
}


const dbConfig = {
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DATABASE,
  password: process.env.DB_PASSWORD,
  port:process.env.DB_PORT,
  ssl: process.env.DB_SSL, 
//   idleTimeoutMillis: 10000000,
};

// a generic query, that executes all queries you send to it
async function query(Query) {
  var result = null;
  const pool = new Pool(dbConfig);
  // appconfig.printlog(context, "Query used", Query)
  await pool.query(Query, (err, res) => {
    // await pool.query("select mastertag from engine ", (err, res) => {
    // appconfig.printlog(context, "db err", err)
    if (err != null) appconfig.printlog(context, "client.query():", err);
    else {
      // appconfig.printlog(context, "Total records :", res.rowCount);
      // appconfig.printlog(context, "res=", res)
      result = res;
    }
  });
  await pool.end();
  return result;
}

function printlog (context, messagePrefix, data) {
  if (!context) {
    if (data === undefined) {
      console.log("VOLVO " + messagePrefix)
    } else {
      console.log("VOLVO " + messagePrefix, data)
    }
  
  } else {
    if (data === undefined) {
      context.log(`VOLVO ${messagePrefix}`)
    } else {
      context.log(`VOLVO ${messagePrefix}: ${data}`);
    }
  }
}

module.exports = {
  query,
  config,
  printlog
};
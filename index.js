require('dotenv').config();
const volvo = require('./generateVolvoApi');
const appconfig = require('./settings')

// const notificationMessage= require("./message").message; // UNCOMMENT OUT WHEN TEST LOCALLY
// main(); // UNCOMMENT OUT when test locally with message.js

async function postVolvoMessage(context, message) {

    // appconfig.printlog(context, "env", process.env)
     // Assuming the message is JSON formatted
    //  const serviceBusConnectionString = process.env.SERVICE_BUS_CON_STRING;
    //  appconfig.printlog(context, 'Service Bus Connection String', serviceBusConnectionString);
     try {
        const messageBody = JSON.parse(message.body);
    
        // Extract data from the message
        const data = messageBody.data;
    
        // Log the data or perform some processing
        appconfig.printlog(context, "postVolvoMessage:Processing data:",  JSON.stringify(data));

        // url":"https://volvop.blob.core.windows.net/devp1/MJVKZCPQ/MJVKZCPQ_20240821220411_15min_Volvo.json.gz"
        let blobUrl = data.url; 

       const urlParts = new URL(blobUrl);
       const containerName = urlParts.pathname.split('/')[1]; // Extract container name
       const blobName = urlParts.pathname.split('/').slice(2).join('/'); // Extract blob name
       appconfig.printlog(context, "postVolvoMessage:blobName", blobName);
       await volvo.processBlobData(context, containerName, blobName);
    //    await volvo.processBlobData(context, "MJCKZCPQ/blobVolvoWithEngineHours_1s_2signal.json.gz");
    } catch (error) {
        let result = {
            code : error.code,
            responseStatus : error?.response?.status, 
            responseStatusText: error?.response?.statusText
        }
       appconfig.printlog(context, "VOLVO ComponentReadout error", result)
    }
     
}

module.exports = async function run(context, message) {
    var timeStamp = new Date().toISOString();
    // intercept(context);
    appconfig.printlog(context, "running from run taking service bus message ", JSON.stringify(message))

    await postVolvoMessage(context, message);
     
    appconfig.printlog(context, 'Notification receiver function ran!  Started at', timeStamp);
    context.done();
};

async function main() {
    appconfig.printlog(null, "Executing main")
    var timeStamp = new Date().toISOString();
   // intercept(context);
//    console.log("process.env", process.env)

     await postVolvoMessage(null, notificationMessage);
     
    appconfig.printlog(null, 'JavaScript main ran! started at', timeStamp);
  }

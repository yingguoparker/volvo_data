
//   start on next block
const appconfig = require('./settings');
const https = require('https');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid')
const moment = require('moment');
const dbManager = require('./dbManager');
const fs = require('fs');
const { BlobServiceClient } = require("@azure/storage-blob");
const zlib = require('zlib');

const FormData = require('form-data');
const cache = require('./redis');
const token_cache_expiry_seconds = 3000; // 50 minutes

const dictionaryToArrayOfObjects = (dict) => {
  return Object.entries(dict).
      map(([key, v]) => ({ SPN: Number(key), value: Number(v) }));
};

 
const generateVolvoApiCanData = async function (mastertag, pgnSignalMap) {
  // TODO remove below hard-coded data, and use mastertag instead

  let engineDetail = await cache.get(mastertag + ":engine");
  // appconfig.printlog(context, "engineDetail", engineDetail)
  let chassis = Number(engineDetail.chassisNumber || '0');
  // TODO fix the data 45206 is a bad data to force redis update test
  if (chassis === 456206) {
    chassis = 456205
  }
  let series = engineDetail.seriesName; 
  // appconfig.printlog(context, "chassis", chassis)
  // appconfig.printlog(context, "series", series)
  let assetCanSignals = []; 

  for( let [pgnKey, signalGrp] of pgnSignalMap) {
    let keyArray = pgnKey.split(":");
    let pgn = 0;
    let spn = 0; 
    let tsUtcNumber =0;
    let sa = 0; 

    // appconfig.printlog(context, "keyArray.length")
    if (keyArray.length > 1) {
       pgn = Number(keyArray[1]);
    }

    // appconfig.printlog(context, "----------------- keyArray[1]", keyArray[1]);
    // appconfig.printlog(context, "----------------- pgn", pgn)

    if (keyArray.length > 2) {
        sa = Number(keyArray[2]);
    }

    if (keyArray.length > 3) {
       tsUtcNumber = Number(keyArray[3]);
    }

    // appconfig.printlog(context, "tsUtcNumber", tsUtcNumber);
    let timestampIso = new Date(tsUtcNumber);
    // appconfig.printlog(context, "tsDate", timestampIso)
    let signalArray = dictionaryToArrayOfObjects(signalGrp); 
    // appconfig.printlog(context, "signalArray", signalArray)
  
     if (pgn !== 0 ) { // && pgn !== 64891) {
    // if (pgn === 65257) { // 65257, 61443, 61444. 65270, 65263, 65266, 65262, '61444 OK
    let CANSignals = {
      "PGN": pgn,
      "sourceAddress": sa,
      "timestamp": timestampIso,
      "signals": signalArray
    }
    assetCanSignals.push(CANSignals);   
    }
  }          

  let asset = {
    assetReference: mastertag,
    driveline: {
        chassisSeries: series,
        chassisNumber: chassis
    },
    CANSignals: assetCanSignals
  }

  let msgUuid = uuidv4();
  // appconfig.printlog(context, 'Message UUID is: ', Date.now());
   
  let reqMessageHeader = {
        messageId:msgUuid,
        timestamp: new Date(Date.now())
  }

  // appconfig.printlog(context, "reqMessageHeader", reqMessageHeader);
  let dataArray = []; 
  dataArray.push(asset);

  let reqPayload = {
    messageHeader : reqMessageHeader,
    data: dataArray
  }

  appconfig.printlog(context, "reqPayload", JSON.stringify(reqPayload));

  return reqPayload; 
}



 
const generateVolvoApiCanDataHeader = async function (mastertag, pgnSignalMap) {
  // TODO remove below hard-coded data, and use mastertag instead

  let msgUuid = uuidv4();
  // appconfig.printlog(context, 'Message UUID is: ', Date.now());
   
  let reqMessageHeader = {
        messageId:msgUuid,
        timestamp: new Date(Date.now())
  }

  // appconfig.printlog(context, "reqMessageHeader", reqMessageHeader);
  let dataArray = []; 
  dataArray.push(asset);

  let reqPayload = {
    messageHeader : reqMessageHeader,
    data: dataArray
  }

  // appconfig.printlog(context, "reqPayload", JSON.stringify(reqPayload));

  return reqPayload; 
}



 
const transformVolvoApiCanData = async function (rawDbRow, engineDetail) {
  // TODO remove below hard-coded data, and use mastertag instead

  let chassis = Number(engineDetail.chassisNumber || '0');
  // TODO fix the data 45206 is a bad data to force redis update test
  if (chassis === 456206) {
    chassis = 456205
  }
  let series = engineDetail.seriesName; 
  // appconfig.printlog(context, "chassis", chassis)
  // appconfig.printlog(context, "series", series)
  let assetCanSignals = []; 

  for( let [pgnKey, signalGrp] of pgnSignalMap) {
    let keyArray = pgnKey.split(":");
    let pgn = 0;
    let spn = 0; 
    let tsUtcNumber =0;
    let sa = 0; 

    // appconfig.printlog(context, "keyArray.length")
    if (keyArray.length > 1) {
       pgn = Number(keyArray[1]);
    }

    // appconfig.printlog(context, "----------------- keyArray[1]", keyArray[1]);
    // appconfig.printlog(context, "----------------- pgn", pgn)

    if (keyArray.length > 2) {
        sa = Number(keyArray[2]);
    }

    if (keyArray.length > 3) {
       tsUtcNumber = Number(keyArray[3]);
    }

    // appconfig.printlog(context, "tsUtcNumber", tsUtcNumber);
    let timestampIso = new Date(tsUtcNumber);
    // appconfig.printlog(context, "tsDate", timestampIso)
    let signalArray = dictionaryToArrayOfObjects(rawDbRow.signal); 
    // appconfig.printlog(context, "signalArray", signalArray)
  
     if (pgn !== 0 ) { // && pgn !== 64891) {
    // if (pgn === 65257) { // 65257, 61443, 61444. 65270, 65263, 65266, 65262, '61444 OK

    let CANSignals = {
      "PGN": rawDbRow.pgn,
      "sourceAddress": rawDbRow.sa,
      "timestamp": rawDbRow.time,
      "signals": signalArray
    }
    assetCanSignals.push(CANSignals);   
    }
  }          

  let asset = {
    assetReference: mastertag,
    driveline: {
        chassisSeries: series,
        chassisNumber: chassis
    },
    CANSignals: assetCanSignals
  }

  let msgUuid = uuidv4();
  // appconfig.printlog(context, 'Message UUID is: ', Date.now());
   
  let reqMessageHeader = {
        messageId:msgUuid,
        timestamp: new Date(Date.now())
  }

  // appconfig.printlog(context, "reqMessageHeader", reqMessageHeader);
  let dataArray = []; 
  dataArray.push(asset);

  let reqPayload = {
    messageHeader : reqMessageHeader,
    data: dataArray
  }

  // appconfig.printlog(context, "reqPayload", JSON.stringify(reqPayload));

  return reqPayload; 
}

// const agent = new https.Agent({  
//     rejectUnauthorized: false
//   });
// axios.defaults.httpsAgent = agent;



const agent = new https.Agent({
    rejectUnauthorized: false,
  })



async function getToken(context) {

  let shadowTokenKey = 'volvopenta:token';
  try{        
      const access_token =  await cache.loadCache(context, shadowTokenKey, token_cache_expiry_seconds);
      // appconfig.printlog(context, "shadow access_token", access_token)
      if(access_token) {
          return access_token;
      }
  }
  catch(error){
      // appconfig.printlog(context, `Failed to get ${shadowTokenKey} from Redis`);
      appconfig.printlog(context, 'Error getting token from Redis:', error);
  }

    // appconfig.printlog(context, "appconfig.config", appconfig.config)
    const url = appconfig.config.volvo_token_api_url;
    const form = new FormData();
    form.append('grant_type', 'client_credentials');
    form.append('client_id', appconfig.config.volvo_token_api_client_id);
    form.append('client_secret', appconfig.config.volvo_token_api_client_secret);
    form.append('scope', appconfig.config.volvo_token_api_scope);
    // appconfig.printlog(context, "token Form", form)

/*
    ==== QA TEST example ==========
    MJVKZCPQ- QA mastertag
    "chassisSeries": "RIGP",
    "chassisNumber": "100541"
    ====
*/
    try {
        const response = await axios.post(url, form, {
            headers: {
                ...form.getHeaders(),
                  'Cookie': appconfig.config.volvo_token_api_cookie
            }           
        });

        if(response.data.access_token){
            // appconfig.printlog(context, "API access_token", response.data.access_token)
            await cache.storeCache(context, shadowTokenKey, response.data.access_token, token_cache_expiry_seconds); 
        }

        // appconfig.printlog(context, "Volvo API token", response.data.access_token)
        return response.data.access_token;
    } catch (error) {
      let result = {
        code : error.code,
        responseStatus : error?.response?.status || 500, 
        responseStatusText: error?.response?.statusText ||"Internal Server Error"
      }
      if (!context) {
        appconfig.printlog(context, "getToken error", JSON.stringify(result))
      } else {
        appconfig.printlog(context, "getToken error", JSON.stringify(result))
      }
    }
}

async function postComponentReadouts(data, token, context) {

  try {
    const url = appconfig.config.volvo_api_url;
    const response = await axios.post(url, data, {
        headers: {
            'Authorization': `Bearer ${token}`,
            'X-IBM-Client-Id': appconfig.config.volvo_api_client_id,
            'X-IBM-Client-Secret': appconfig.config.volvo_api_client_secret,
            'Content-Type': 'application/json'
        },
        // httpsAgent: agent            
    });

    // appconfig.printlog(context, "response.status", response.status)

      return response.status;
    } catch (error) {
      let result = {
        code : error.code,
        responseStatus : error?.response?.status || 500, 
        responseStatusText: error?.response?.statusText ||"Internal Server Error"
      }
      appconfig.printlog(context, "postComponentReadout API error", JSON.stringify(result));
        // context.error('Error getting component readouts:', error.response ? error.response.data : error.message);
      return result.responseStatus;  
    }
}



async function bulkPostComponentReadouts(deviceDataArray, token, context) {
      
      return new Promise(async (resolve, reject) => {
          try {
              if ((deviceDataArray.length || 0) === 0) {
                  resolve([]);
              }
            
              let promises = [];
  
            
              for (let d = 0; d < deviceDataArray.length; d++) {
                  let deviceData = deviceDataArray[d];
                  const size = new TextEncoder().encode(JSON.stringify(deviceData)).length; 
                  // appconfig.printlog(context, "API request " + size + " bytes")
                  if (size < 10) {
                    appconfig.printlog(context, "empty readout API request", JSON.stringify(deviceData))
                  } else {
                    promises.push( postComponentReadouts(deviceData, token, context));
                  }
                 
              }
  
              let requestStartTs = Date.now();
              return Promise.all(promises)
                  .then((respArray) => {        
                      // Add the empty AEMP entries for those lack of the report DB entries
                      let result = [];
                      respArray.forEach(compReadResp => {
                          result.push(compReadResp);
                      });
  
                      let requestEndTs = Date.now();
                      let durationInMs = requestEndTs - requestStartTs; 
                      if (!context) {
                          appconfig.printlog(context, "Current group bulk Penta API in : " + durationInMs + " ms, bulkResult=",result);
                      } else {
                          appconfig.printlog(context, "Current group bulk Penta API in : " + durationInMs + " ms, bulkResult=", result);
                      }
                      
                      resolve(result);
                  }).catch(err => {
                      throw(err);
                  });
     
          } catch (error) {
              reject(error);
          }
      }).catch (err2 => {
          throw ( err2);
      });
  }

  async function processVolvoApi(context) { //2207700 rows for 15 minutes 223 gateways
    let lastProcessTime = await cache.getLastPostTime(context);
    appconfig.printlog(context, "lastProcessTime from redis", lastProcessTime)
    // TODO test data
    lastProcessTime = "2024-08-21T22:04:12.000Z"; 
    let groupStartTime = lastProcessTime;  
    appconfig.printlog(context, "API lastProcessTime",  lastProcessTime)
    let oneGroupDurationMinutes = 15; //0; //250 // 640; 
        // TODO change below 2 hours to 15 minutes
 
    // let durationSeconds = 20; // 
    durationSeconds = oneGroupDurationMinutes * 60; 
    //  durationSeconds = 5;
    let volvoToken = await this.getToken(context);
    
    let groups = 1; 
  
    for (let i=0; i< groups; i++) {
      groupEndTime =  new Date(new Date(groupStartTime).getTime() + durationSeconds * 1000).toISOString(); 
      appconfig.printlog(context, "group " + i + " from " + groupStartTime + " to " + groupEndTime)
      await processApiGroupVolvoData(context, groupStartTime, groupEndTime, volvoToken, i); 
      // await processApiGroupVolvoMsg(context, groupStartTime, groupEndTime, volvoToken, i); 
      groupStartTime = groupEndTime; 
    }
  
  }

  
  async function processApiGroupVolvoData(context, batchStartTime, batchEndTime, volvoToken, groupId) {
 

    // 1 or 15 minute 60000 or 900000, function app interval
     
        let dbBatchStartEndTime = batchStartTime.replace('T', ' ');
        // appconfig.printlog(context, "batchEndTime", batchEndTime)
        let dbBatchEndTime = batchEndTime.replace('T', ' ');
        // appconfig.printlog(context, "dbBatcchStartTime", dbBatchStartEndTime)
        // appconfig.printlog(context, "dbBatchEndTime", dbBatchEndTime)
        let diffMillis
    
     
        try {
          
          let beforeDbTime = new Date(); 
          let dbDeltaVolvoData = await dbManager.getVolvoData(context, dbBatchStartEndTime, dbBatchEndTime );
          let diffDbMillis = 0;
      
          diffDbMillis = moment.duration(moment.utc().diff(moment.utc(beforeDbTime))).asMilliseconds(); 
          //  appconfig.printlog(context, "Get DB delta in " + JSON.stringify(diffMillis) + "ms")
           if (dbDeltaVolvoData === undefined || dbDeltaVolvoData === null) {
            appconfig.printlog(context, "No data in time range, DB query in " + diffDbMillis + " ms")
            return; 
           } 
    
           
          let rowCount = dbDeltaVolvoData.rowCount || 0; 
           appconfig.printlog(context, "Group " + groupId + " DB " + rowCount + " rows in " + diffDbMillis + " ms")
    
           if (rowCount === 0) {
            appconfig.printlog(context, "Group " + groupId + " DB rowCount = 0")
            return; 
          }
    
          beforeFormattingTime = new Date(); 
          // let dbDeltaVolvoData = {
          //   rows:  [{
          //   mastertag: "VPGHTY2R",
          //   series: "VP", 
          //   chassis: "56205",
          //   pgn: 51444, 
          //   time: "2024-08-19T15:00:00.321Z",
          //   signal: '[{\"SPN\": 190, \"value\": 37.2}, {\"SPN\": 513, \"value\": 200.4}]'
          // }]}
         
    
          
          let batches = 1; 
          let batchSize = Math.ceil(rowCount / batches); // TODO changes to 8
          // appconfig.printlog(context, "batchSize", batchSize)
          let bulkApiPayloadArray = []; 
          for (let i=0; i< batches; i++) { 
            let assetMap = new Map();
            let startIdx = i*batchSize; 
            let endIdx; 
            if (startIdx + batches > rowCount) {
              endIdx = rowCount;
            } else {
              endIdx = startIdx + batchSize; 
            }
    
            appconfig.printlog(context, "Group " + groupId + " batch " + i + " DB rows [ " + startIdx + " - "+ endIdx + " ]");
            for (let idx = startIdx; idx< endIdx; idx++) {
                let dbRaw = dbDeltaVolvoData.rows.at(idx); 
                
                if (dbRaw === undefined || dbRaw === null) {
                  appconfig.printlog(context,  "Group " + groupId + " batch " + i + " db idx " + idx + " null val: ", JSON.stringify(dbRaw));
                } else {
              
                  // let mastertag = dbRaw.mastertag || ""; 
                  let mastertag = 'MJVKZCPQ'; 
        
                  let assetVal = assetMap.get(mastertag);
                  let assetCanSignals = []; 
                  if (assetVal === undefined || assetVal === null) {
                    assetCanSignals = []; 
                    assetVal = {
                      assetReference:  dbRaw.mastertag,
                      driveline: {
                          chassisSeries: "RIGP", // dbRaw.series,
                          chassisNumber: "100541", // dbRaw.chassis
                      },
                      CANSignals: assetCanSignals
                    }
                  } else {
                    assetCanSignals = assetVal.CANSignals; 
                  }
      
                  // appconfig.printlog(context, "dbRaw.signal", dbRaw.signal);
              
                  let newAssetCanSignal = {
                    PGN: dbRaw.pgn, 
                    sourceAddress: dbRaw.sa,
                    timestamp : dbRaw.time, 
                    signals: dbRaw.signal
                  }
                  assetCanSignals.push(newAssetCanSignal);
                  assetVal.CANSignals = assetCanSignals; 
                  assetMap.set(mastertag, assetVal);
                  // appconfig.printlog(context, "assetMap", assetMap)
                }
               
            }
    
    
            // appconfig.printlog(context, "reqMessageHeader", reqMessageHeader);
            let dataArray = []; 
    
            for (let [mastertag, assetVal] of assetMap) {
              dataArray.push(assetVal); 
            }
    
            if (dataArray.length > 0) {
              // appconfig.printlog(context, "dataArray", JSON.stringify(dataArray));
              // appconfig.printlog(context, "reqMessageHeader", reqMessageHeader)
              let msgUuid = uuidv4();
              // appconfig.printlog(context, 'Message UUID is: ', Date.now());
                 
              let reqMessageHeader = {
                messageId:msgUuid,
                timestamp: new Date(Date.now())
              }
              
              let reqPayload = {
                messageHeader : reqMessageHeader,
                data: dataArray
              }
    
              // appconfig.printlog(context, "reqPayload", JSON.stringify(reqPayload))
              diffMillis = moment.duration(moment.utc().diff(moment.utc(beforeFormattingTime))).asMilliseconds(); 
              appconfig.printlog(context, "Group " + groupId + " formatting in " + diffMillis + " ms")
              
              // appconfig.printlog(context, "reqPayload= ", reqPayload);
    
              const pailoadBytes = new TextEncoder().encode(JSON.stringify(reqPayload)).length
              const payloadKb = pailoadBytes / 1024;
              // const payloadMb = payloadKb / 1024;
    
              // appconfig.printlog(context, "payload in " +  payloadKb.toFixed(3) +  "kb");
              // appconfig.printlog(context, "payloadMb", payloadMb)
    
              bulkApiPayloadArray.push(reqPayload);
              // await postComponentReadouts(reqPayload, volvoToken, context);
            }
          }
          let beforeApiTime = new Date();
      
          // appconfig.printlog(context, "bulkApiPayloadArray.legth", bulkApiPayloadArray.length)
          if (bulkApiPayloadArray.length > 0) {
              await bulkPostComponentReadouts(bulkApiPayloadArray, volvoToken, context);
          } else {
            appconfig.printlog(context, "skip the bulk API call since bulkApiPayloadArray.length is 0")
          }
        
          diffMillis = moment.duration(moment.utc().diff(moment.utc(beforeApiTime))).asMilliseconds(); 
      
          appconfig.printlog(context, "volvoAPI in " + diffMillis + " ms")
    
        } catch (error) {
          appconfig.printlog(context, "index postComponentReadOuts error", error)
        }
         
    }
    

  async function genDeviceBlobPayload(context) {
    let canSigArraySec1 = [
      {
          PGN: 61443,
          sourceAddress: 0,
          timestamp: "2024-08-21T22:04:11.000Z",
          signals: [
              {SPN: 92, value: 100.0}
          ]
      }, 
      {
          PGN: 61444,
          sourceAddress: 0,
          timestamp: "2024-08-21T22:04:11.000Z",
          signals: [
              {SPN: 190, value: 1936.25},
              {SPN: 513, value: 65.0}
          ]
      }, 
      {
          PGN: 65262,
          sourceAddress: 0,
          timestamp: "2024-08-21T22:04:11.000Z",
          signals: [
              {SPN: 100, value: 84.0},
              {SPN: 175, value: 100.8125}
          ]
      }, 
      {
          PGN: 65263,
          sourceAddress: 0,
          timestamp: "2024-08-21T22:04:11.000Z",
          signals: [
              {SPN: 94, value: 520.0},
              {SPN: 100, value: 424.0}
          ]
      }, 
      {
          PGN: 65270,
          sourceAddress: 0,
          timestamp: "2024-08-21T22:04:11.000Z",
          signals: [
              {SPN: 102, value: 146.0},
              {SPN: 105, value: 58.0},
              {SPN: 173, value: 317.53125}
          ]
      }, 
      {
          PGN: 65266,
          sourceAddress: 0,
          timestamp: "2024-08-21T22:04:11.000Z",
          signals: [
              {SPN: 183, value:  68.60000000000001}
          ]
      }, 
      {
          PGN: 57344,
          sourceAddress: 0,
          timestamp: "2024-08-21T22:04:11.875",
          signals: [
              {SPN: 3695, value: 3.0},
              {SPN: 3696, value: 3.0}
          ]
      },
      {
          PGN: 65252,
          sourceAddress: 0,
          timestamp: "2024-08-21T22:04:11.875Z",
          signals: [
              {SPN: 1109, value: 0.0},
              {SPN: 1110, value: 0.0}
          ]
      }
  ];
appconfig.printlog(context, "blob -----------------------")
  try {
    
    let beforeDateTime = new Date(); 

    let signalCount = 0; 
    let assetCanSignals = []; 
    for (let i=0; i< 900; i++) { 
      let durationSeconds = i;
      for (let j=0; j< 8; j++) { // unique pgns are 8
        let pgnRecord = canSigArraySec1.at(j);
        if (pgnRecord === undefined || pgnRecord === null) {
          appconfig.printlog(context, "signal index is null for idx ", j);
          continue
        }
        let newTimeStr = new Date(new Date(pgnRecord.timestamp).getTime()  + durationSeconds * 1000).toISOString(); 
        //  appconfig.printlog(context, "newTimeStr", newTimeStr);
        
        let newPgnSig = {
          PGN: pgnRecord.PGN,
          sourceAddress: pgnRecord.sourceAddress,
          timestamp: newTimeStr,
          signals: pgnRecord.signals
        }
        // appconfig.printlog(context, "newPgnSig")
        assetCanSignals.push(newPgnSig)
        signalCount += pgnRecord.signals.length;
      }
    }

    let assetVal =  {
      assetReference: "MJVKZCPQ",
      driveline: {
          chassisSeries: "RIGP",
          chassisNumber: "100541"
      },
      CANSignals: assetCanSignals
    }
            
    let reqMessageHeader = {
      messageId: uuidv4(),
      timestamp: new Date(Date.now())
    }
      
    let dataArray = []
    dataArray.push(assetVal);

    let reqPayload = {
      messageHeader : reqMessageHeader,
      data: dataArray
    }

    // appconfig.printlog(context, "blobReqPayload= ", JSON.stringify(reqPayload));
   
    fs.writeFile("blobVolvo.json", JSON.stringify(reqPayload, null, 0), (err) => {
      if (err) {
        appconfig.printlog(context, 'Error writing to file:', err);
      } else {
          appconfig.printlog(context, 'JSON data written to output.json in compressed format.');
      }
  });
      
    diffMillis = moment.duration(moment.utc().diff(moment.utc(beforeDateTime))).asMilliseconds(); 
    appconfig.printlog(context, "blob device raw prep in " + diffMillis + " ms")
      
    // appconfig.printlog(context, "signalCount", signalCount)
    const payloadBytes = new TextEncoder().encode(JSON.stringify(reqPayload)).length
    const payloadKb = payloadBytes / 1024;
    // const payloadMb = payloadKb / 1024;

    appconfig.printlog(context, "payload in " +  payloadKb.toFixed(3) +  "kb");
    // appconfig.printlog(context, "payloadMb", payloadMb)

    let beforeApiTime = new Date();
    await postComponentReadouts(reqPayload, volvoToken, context);
  
    diffMillis = moment.duration(moment.utc().diff(moment.utc(beforeApiTime))).asMilliseconds(); 

    appconfig.printlog(context, "volvoAPI for 1 device in " + diffMillis + " ms")

  } catch (error) {
    appconfig.printlog(context, "index postComponentReadOuts error", error)
  } 
}


function extractApiDetails(context, jsonDataStr) {
  let jsonData = JSON.parse(jsonDataStr); 
  // Loop through the groups to find the desired PGN or SPN
  // appconfig.printlog(context, "jsonData", jsonData)
  if ((jsonData.data || []).length === 0) {
    return {};
  }

  let apiTimestamp = jsonData.messageHeader.timestamp;
  let asset = jsonData.data.at(0);


  let signalCount = 0; 
  let engineHour= null;
  // appconfig.printlog(context, "asset", asset.assetReference)

  for (const canSignals of asset.CANSignals) {
      // Check for PGN 65253
      signalCount += canSignals.signals.length; 
      if (canSignals.PGN === 65253) {
          // Return the value from the first signal (only one SPN 247)
          engineHour = canSignals.signals[0].value;
      }
  }

  let result = {mastertag: asset.assetReference, 
      apiTimestamp: apiTimestamp,  
      engineHour: engineHour, 
      signalCount: signalCount};
      appconfig.printlog(context, "extracted as ", JSON.stringify(result));
  return  result; // Assumes there's only one signal for PGN ; // Return null if neither is found
}



async function downloadBlob(context, containerName, blobName) {
  // appconfig.printlog(context, "downloading blob")
  let beforeApiTime = new Date();

  // appconfig.printlog(context, "appconfig.config.blob_storage_con_string", appconfig.config.blob_storage_con_string)

  const blobServiceClient = BlobServiceClient.fromConnectionString(appconfig.config.blob_storage_con_string);
  // const containerClient = blobServiceClient.getContainerClient(appconfig.config.container_name);
  const containerClient = blobServiceClient.getContainerClient(containerName);
  // blobName = "MJVKZCPQ/MJVKZCPQ_20240821220411_15min_Volvo.json.gz";
  // blobName = "MJVKZCPQ/MJVKZCPQ_20240821220411_15min_EngineHour_Volvo.json.gz";
  // blobName = 'MJVKZCPQ/blobVolvoWithEngineHours_1s_2signal.json.gz'
  // let storageLocation = appconfig.config.blob_storag_url+ "/"+ blobName;
  // appconfig.printlog(context, "Storage Location: "+ storageLocation);
  const blockblobClient = containerClient.getBlockBlobClient(blobName);
  const downloadResponse = await blockblobClient.download(0);
 
    // Retrieve the properties of the blob
    const properties = await blockblobClient.getProperties();
    
    // Get the size of the blob in bytes
    const gzipSizeInBytes = properties.contentLength; // This is the size of the Gzip file
    
    // appconfig.printlog(context, 'Gzip size in bytes:', gzipSizeInBytes);
  /// Create a Gunzip stream
  
  // Pipe the readable stream into the gunzip stream
  return new Promise((resolve, reject) => {
    // Create a Gunzip stream
    const gunzip = zlib.createGunzip();
    const chunks = [];
      // Pipe the readable stream into the gunzip stream
    downloadResponse.readableStreamBody
    .pipe(gunzip)
    .on('data', (chunk) => {
        chunks.push(chunk); // Collect the uncompressed chunks
    })
      .on('end', () => {
          // Combine all chunks into a single Buffer
          const deviceVolvoJsonData = Buffer.concat(chunks);
          const apiBytes = deviceVolvoJsonData.length;
          // appconfig.printlog(context, "apiBytes", apiBytes)    
          // appconfig.printlog(context, 'Uncompressed data:', deviceVolvoJsonData.toString()); // Convert to string if needed
          
          diffMillis = moment.duration(moment.utc().diff(moment.utc(beforeApiTime))).asMilliseconds(); 

          appconfig.printlog(context, "download and unzip for 1 device in " + diffMillis + " ms")
          // appconfig.printlog(context, "payload in " +  payloadKb.toFixed(3) +  "kb");
          // appconfig.printlog(context, "payloadMb", payloadMb)
          // appconfig.printlog(context, "downloaded from blob data", deviceVolvoJsonData.toString())
          let result = {
            compressedBytes: gzipSizeInBytes,
            apiBytes: apiBytes, 
            reqPayload: deviceVolvoJsonData.toString()
          }
         resolve(result); 
      })
      .on('error', (err) => {
          reject('Error during uncompression:' + err);
      });
    });

}


async function processBlobData(context, containerName, blobName) {
  
  try {
    // reqPayload is one device 15 minutes data in Volvo Penta API payload format
    let downloadResult = await this.downloadBlob(context, containerName, blobName)
    let apiBytes = downloadResult.apiBytes; 
    // appconfig.printlog(context, "apiBytes", apiBytes)
    let attBytes = downloadResult.compressedBytes
    let reqPayload = downloadResult.reqPayload; 
  /*  let reqPayload = {
      "messageHeader":{"messageId":"e1bc5eea-d1bf-45b9-94e6-cd8417f35601", "timestamp":"2024-09-03T20:54:22.874Z"},
      "data":[{"assetReference":"MJVKZCPQ","driveline":{"chassisSeries":"RIGP","chassisNumber":"100541"},
        "CANSignals":[
          {"PGN":61444,"sourceAddress":0,"timestamp":"2024-08-21T22:04:11.000Z", "signals":[{"SPN":190,"value":1936.25},{"SPN":513,"value":65}]}, 
          {"PGN":65253,"sourceAddress":0,"timestamp":"2024-08-21T22:04:11.000Z", "signals":[{"SPN":247,"value":20000}]}
        ]}             
      ]} */
    
    let beforeDateTime = new Date(); 
    const details = extractApiDetails(context, reqPayload);
    let apiTimestamp = details.apiTimestamp; 
    let engineHour = details.engineHour; 
    let signalCount = details.signalCount; 
    let mastertag = details.mastertag; 
  
    if (mastertag !== undefined && mastertag !== null && engineHour !== undefined && engineHour !== null) {
      await cache.set(mastertag + ":engine", engineHour)
    }
    
    diffMillis = moment.duration(moment.utc().diff(moment.utc(beforeDateTime))).asMilliseconds(); 
    appconfig.printlog(context, "extract engineHour takes " + diffMillis + " ms, found ", engineHour)
  

    // appconfig.printlog(context, "reqPayload", reqPayload)
    beforeDateTime = new Date(); 
    const payloadKb = apiBytes / 1024;
    // const payloadMb = payloadKb / 1024;

    let successfulReadOutCount = 0; 
    let failedReadoutCount = 0; 
    let statusCode = 500; 
    try {
      let volvoToken = await this.getToken(context);
      statusCode = await postComponentReadouts(reqPayload, volvoToken, context);

      if (statusCode === 200) {
        successfulReadOutCount = 1; 
      } else {
        failedReadoutCount = 1; 
      }
    
    } catch (e) {
      appconfig.printlog(context, "Volvo Penta API error", e)
      failedReadoutCount = 1; 
    }
    diffMillis = moment.duration(moment.utc().diff(moment.utc(beforeDateTime))).asMilliseconds(); 
    appconfig.printlog(context, "Volvo API for " + mastertag + " : " + signalCount + " signals, " + " gzip " + (attBytes / 1000) + " kb, API payload " + payloadKb.toFixed(3)  + " kb with status " + statusCode + " in " + diffMillis + " ms")
      
    // appconfig.printlog(context, "payload in " +  payloadKb.toFixed(3) +  "kb");
    // appconfig.printlog(context, "payloadMb", payloadMb)

    await dbManager.insertOrUpdateVolvoBilling(context, mastertag, successfulReadOutCount,
      failedReadoutCount, 0, 0, attBytes,
        apiBytes, signalCount, apiTimestamp.replace('T', ' ').replace('Z', '').toString() );

  } catch (error) {
    appconfig.printlog(context, "index postComponentReadOuts error", error)
  } 
}

async function processApiGroupVolvoMsg(context, batchStartTime, batchEndTime, volvoToken, groupId) {
 

  // 1 or 15 minute 60000 or 900000, function app interval
   
      let dbBatchStartEndTime = batchStartTime.replace('T', ' ');
      // appconfig.printlog(context, "batchEndTime", batchEndTime)
      let dbBatchEndTime = batchEndTime.replace('T', ' ');
      // appconfig.printlog(context, "dbBatcchStartTime", dbBatchStartEndTime)
      // appconfig.printlog(context, "dbBatchEndTime", dbBatchEndTime)
      let diffMillis
  
   
      try {
        
        let beforeDbTime = new Date(); 
        let dbDeltaVolvoData = await dbManager.getVolvoMsg(context, dbBatchStartEndTime, dbBatchEndTime );
        let diffDbMillis = 0;
    
        diffDbMillis = moment.duration(moment.utc().diff(moment.utc(beforeDbTime))).asMilliseconds(); 
        //  appconfig.printlog(context, "Get DB delta in " + JSON.stringify(diffMillis) + "ms")
         if (dbDeltaVolvoData === undefined || dbDeltaVolvoData === null) {
          appconfig.printlog(context, "No data in time range, DB query in " + diffDbMillis + " ms")
          return; 
         } 
  
         
        let rowCount = dbDeltaVolvoData.rowCount || 0; 
         appconfig.printlog(context, "Group " + groupId + " DB " + rowCount + " rows in " + diffDbMillis + " ms")
        //  appconfig.printlog(context, "dbDelaVolvoData", JSON.stringify(dbDeltaVolvoData.rows))

        if (rowCount === 0) {
          appconfig.printlog(context, "Group " + groupId + " DB rowCount = 0")
          return; 
        }
  
        beforeFormattingTime = new Date(); 
        // option 1 from db volvodata table which contains pgn per row
        // each row contains 1 .. 3 SPNs for the 1s data
        // and there is only one mastertag in the system
        // let dbDeltaVolvoData = {
        //   rows:  [{
        //   mastertag: "VPGHTY2R",
        //   series: "VP", 
        //   chassis: "456205",
        //   pgn: 51444, 
        //   time: "2024-08-19T15:00:00.321Z",
        //   signal: '[{\"SPN\": 190, \"value\": 37.2}, {\"SPN\": 513, \"value\": 200.4}]'
        // }]}
        // option 2 from volvomsg table that each message is one mastertag, each msg contains 1 minutes 15 signals. dbDeltavolvoData.rows = 
        // [ 
        //   {
        //    "mastertag":"VPGHTY2R",
        //    "chassis":"456205",
        //    "series":"VP",
        //    "time":"2024-08-21T22:04:11.670Z",
        //    "signal":{
        //      "driveLine":{
        //            "chassisNumber":"456206",
        //            "chassisSeries":"VP"},
        //      "CANSignals":[
        //         {"pgn":"61443",
        //          "signals":[{"SPN":92,"value":100}],
        //          "timestamp":"2024-08-21 22:04:11.670Z",
        //          "sourceAddress":0},
        //         {"pgn":"61444",
        //          "signals":[{"SPN":190,"value":1965.375},{"SPN":513,"value":52}],
        //          "timestamp":"2024-08-21 22:04:11.670Z",
        //          "sourceAddress":0}],
        //          "assetReference":"VPGHTY2R"
        //         },
        //     "signalcount":3
        //    }
        //   ]
       
  
        
        let batches = 1; 
        let batchSize = Math.ceil(rowCount / batches); // TODO changes to 8
        // appconfig.printlog(context, "batchSize", batchSize)
        let bulkApiPayloadArray = []; 
        for (let i=0; i< batches; i++) { 
          let assetMap = new Map();
          let startIdx = i*batchSize; 
          let endIdx; 
          if (startIdx + batches > rowCount) {
            endIdx = rowCount;
          } else {
            endIdx = startIdx + batchSize; 
          }
  
          let dataArray = [];
          let cont = true; 
          appconfig.printlog(context, "Group " + groupId + " batch " + i + " DB rows [ " + startIdx + " - "+ endIdx + " ]");
          for (let idx = startIdx; idx < endIdx & cont; idx++) {
              let dbRaw = dbDeltaVolvoData.rows.at(idx); 
              
              if (dbRaw === undefined || dbRaw === null) {
                appconfig.printlog(context,  "Group " + groupId + " batch " + i + " db idx " + idx + " null val: "); //, JSON.stringify(dbRaw));
                cont = false; 
              } else {
                    // appconfig.printlog(context, "dbRaw for idx " + idx, dbRaw)
                // let mastertag = dbRaw.mastertag || ""; 
      
                // let assetVal = assetMap.get(mastertag);
                // let assetCanSignals = []; 
                // if (assetVal === undefined || assetVal === null) {
                //   assetCanSignals = []; 
                //   assetVal = dbRaw.signal; 
                //   mastertag = dbRaw.mastertag; 
                // } else {
                //   assetCanSignals = assetVal.CANSignals; 
                // }
    
                // appconfig.printlog(context, "dbRaw.signal", dbRaw.signal);
            
                // let newAssetCanSignal = {
                //   PGN: dbRaw.pgn, 
                //   sourceAddress: dbRaw.sa,
                //   timestamp : dbRaw.time, 
                //   signals: dbRaw.signal
                // }
                // appconfig.printlog(context, "dbRaw.signal", dbRaw.signal)
                let signalStr = JSON.stringify(dbRaw.signal || '[]'); 
                let assetValModifiedStr = signalStr.replace("VPGHTY2R", 'MJVKZCPQ').replace("VP", "RIGP").replace("456206", "100541"); 
                let assetVal = JSON.parse(assetValModifiedStr)
                // appconfig.printlog(context, "assetVal", JSON.stringify(assetVal)); 
                // assetCanSignals.push(newAssetCanSignal);
                // assetVal.CANSignals = assetCanSignals; 
                // assetMap.set(mastertag, assetVal);
                // appconfig.printlog(context, "assetMap", JSON.stringify(assetMap))
                dataArray.push(assetVal)
              }
             
          }
  
          if (dataArray.length > 0) {
            let msgUuid = uuidv4();
            // appconfig.printlog(context, 'Message UUID is: ', Date.now());
               
            let reqMessageHeader = {
              messageId:msgUuid,
              timestamp: new Date(Date.now())
            }
            
            // appconfig.printlog(context, "reqMessageHeader", reqMessageHeader);
            // let dataArray = []; 
    
            // for (let [mastertag, assetVal] of assetMap) {
            //   dataArray.push(assetVal); 
            // }
    
            // appconfig.printlog(context, "dataArray", JSON.stringify(dataArray));
            // appconfig.printlog(context, "reqMessageHeader", reqMessageHeader)
          
            let reqPayload = {
              messageHeader : reqMessageHeader,
              data: dataArray
            }
    
            // appconfig.printlog(context, "reqPayload", JSON.stringify(reqPayload))
            diffMillis = moment.duration(moment.utc().diff(moment.utc(beforeFormattingTime))).asMilliseconds(); 
            appconfig.printlog(context, "Group " + groupId + " formatting in " + diffMillis + " ms")
            
            appconfig.printlog(context, "reqPayload= ", JSON.stringify(reqPayload));
    
            const payloadBytes = new TextEncoder().encode(JSON.stringify(reqPayload)).length
            const payloadKb = payloadBytes / 1024;
            // const payloadMb = payloadKb / 1024;
    
            appconfig.printlog(context, "payload in " +  payloadKb.toFixed(3) +  "kb");
            // appconfig.printlog(context, "payloadMb", payloadMb)
    
            bulkApiPayloadArray.push(reqPayload);
            // await postComponentReadouts(reqPayload, volvoToken, context);
          }
           
        }
        
          
        let beforeApiTime = new Date();
     
        // appconfig.printlog(context, "bulkApiPayloadArray.legth", bulkApiPayloadArray.length)
        // appconfig.printlog(context, "bulkApiPayloadArray", JSON.stringify(bulkApiPayloadArray))
        if (bulkApiPayloadArray.length > 0) {
          //  await bulkPostComponentReadouts(bulkApiPayloadArray, volvoToken, context);
        } else {
          appconfig.printlog(context, "skip the bulk API call since bulkApiPayloadArray.length is 0")
        }
      
        diffMillis = moment.duration(moment.utc().diff(moment.utc(beforeApiTime))).asMilliseconds(); 
    
        appconfig.printlog(context, "volvoAPI in " + diffMillis + " ms")
  
      } catch (error) {
        appconfig.printlog(context, "index postComponentReadOuts error", error)
      }
       
  }
  

module.exports = {
  getToken,
  postComponentReadouts, 
  generateVolvoApiCanData, 
  bulkPostComponentReadouts, 
  processVolvoApi,
  genDeviceBlobPayload,
  downloadBlob, 
  processBlobData
};
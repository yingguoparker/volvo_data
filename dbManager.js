const appconfig = require('./settings');

// const infoLogging = process.env.VERBOSE_INFO_LOGGING;


async function getEngine(mastertag)
{// Sync the latest DB cummins OTA status to cleanup the redis map

  let result = null;
  let Query;

  Query =  " select vin, series, chassis from public.engine";
  Query += " where mastertag = '" + mastertag + "';";
  // appconfig.printlog(context, "Query", Query)

  try {

    // appconfig.printlog(context, "appconfig", appconfig.query)
  result =  await  appconfig.query(Query);
  // appconfig.printlog(context, "result", result.rows || null)
  } catch (e) {
    appconfig.printlog(context, "db exception ", e)
  }
  return result;
}

async function insertOrUpdateEngine(mastertag, gateway_id, vin, chassis_number, series) {
  let result = null;
  let Query;
 
    Query = "insert into public.engine (mastertag, gateway_id, vin, chassis_number, series, created_at, updated_at) ";
    Query += " values( '" + mastertag + "','" + gateway_id  + "','" + vin + "','" + chassis_number + "','" + series ;
    Query += "', now(), now() ) ";
    Query += " on conflict(mastertag) do update "; 
    Query += " set vin=EXCLUDED.vin, chassis_number=EXCLUDED.chassis_number, series=EXCLUDED.series, updated=now();";  


    //  appconfig.printlog(context, "Query=" + Query);
     result =  await  appconfig.query(Query);
    return result; 
  } 
 
  async function insertOrUpdateVolvoBilling(context, mastertag, delta_successful_readout_count, delta_failed_readout_count, 
      delta_successful_dtc_count, delta_failed_dtc_count, 
      delta_att_bytes, delta_api_bytes, delta_signals, last_message_sent_timestamp) {
      let result = null;
      let Query;
    
      Query = "select * from volvo_billing_maint('" + mastertag;
      Query +=    "'," + delta_successful_readout_count  + "," + delta_failed_readout_count; 
      Query +=    "," + delta_successful_dtc_count + "," + delta_failed_dtc_count ;
      Query +=    "," + delta_att_bytes + "," + delta_api_bytes ;
      Query +=    "," + delta_signals + ",'" + last_message_sent_timestamp + "') " ;
      
      // appconfig.printlog(context, "Query=" + Query);
      result =  await  appconfig.query(Query);
      return result; 
    } 
   
  
// async function insertVolvoData(collected_at, mastertag, pgn, sa, datapoint) {
//     let result = null;
//     let Query;
 
//     Query = "insert into public.iotdata_volvo (time, mastertag, pgn, sa, signal, receivedat) ";
//     Query += " values(now() "+ "," + mastertag  + "," + pgn + "," + sa + "," + signal + ", now())" ;
   
//     // appconfig.printlog(context, "Query=" + Query);
//        result =  await  appconfig.query(Query);
//     return result; 
// } 



// async function getVolvoData(context, start_time, end_time) {
//   let result = null;
//   let Query;

//   Query = "select v.mastertag, e.chassis, e.series, pgn, time, sa, signal from  public.volvodata v, public.engine e ";
//   Query += " where v.mastertag = e.mastertag and receivedat > '" + start_time + "' and receivedat < '" + end_time + "'";

//   // appconfig.printlog(context, "Query=" + Query);
//   result =  await  appconfig.query(Query);
//   return result; 
// } 
        
// async function getVolvoMsg(context, start_time, end_time) {
//   let result = null;
//   let Query;

//   Query = "select v.mastertag, e.chassis, e.series, time, signal, signalcount from  public.volvomsg v, public.engine e ";
//   Query += " where v.mastertag = e.mastertag and receivedat > '" + start_time + "' and receivedat < '" + end_time + "'";

//   // appconfig.printlog(context, "Query=" + Query);
//   result =  await  appconfig.query(Query);
//   return result; 
// } 

module.exports = {
getEngine,
insertOrUpdateEngine,
insertOrUpdateVolvoBilling,
// insertVolvoData, 
// getVolvoData,
// getVolvoMsg
};

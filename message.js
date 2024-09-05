

// value is the revert of the "--VOLVO PENTA--*VP 456205VOLVO" or "OVLOV502654 PV*--ATNEP OVLOV--"
// gID"7bc64dc0-9fea-11ec-954c-3fe1e081a2ef"
module.exports.message= {
  "label": null,
  "messageId": "some-message-id",
  "sessionId": null,
  "userProperties": {
    "customProperty": "value"
  },
  body: "{\"topic\":\"/subscriptions/6ea5b515-8b8b-49a7-b6ee-8537194438de/resourceGroups/iqan03-dev-rg/providers/Microsoft.Storage/storageAccounts/devota\",\"subject\":\"/blobServices/default/containers/devp1/MJVKZCPQ/MJVKZCPQ_20240821220411_15min_Volvo.json.gz\",\"eventType\":\"Microsoft.Storage.BlobCreated\",\"id\":\"9b74f288-f01e-0028-017a-f97eac06843c\",\"data\":{\"api\":\"PutBlob\",\"requestId\":\"9b74f288-f01e-0028-017a-f97eac000000\",\"eTag\":\"0x8DCC79162A5C385\",\"contentType\":\"application/octet-stream\",\"contentLength\":548,\"blobType\":\"BlockBlob\",\"url\":\"https://volvop.blob.core.windows.net/devp1/MJVKZCPQ/MJVKZCPQ_20240821220411_15min_Volvo.json.gz\",\"sequencer\":\"0000000000000000000000000001B02000000000000ae2dc\",\"storageDiagnostics\":{\"batchId\":\"b47f65f4-c006-002b-007a-f97dab000000\"}},\"dataVersion\":\"\",\"metadataVersion\":\"1\",\"eventTime\":\"2024-08-28T18:44:01.8993801Z\"}"
}

/*
module.exports.message= {
  "label": null,
  "messageId": "some-message-id",
  "sessionId": null,
  "userProperties": {
    "customProperty": "value"
  },
  body: {
    "topic":"/subscriptions/6ea5b515-8b8b-49a7-b6ee-8537194438de/resourceGroups/iqan03-dev-rg/providers/Microsoft.Storage/storageAccounts/devota",
    "subject":"/blobServices/default/containers/devp1/MJVKZCPQ/MJVKZCPQ_20240821220411_15min_Volvo.json.gz",
    "eventType":"Microsoft.Storage.BlobCreated",
    "id":"9b74f288-f01e-0028-017a-f97eac06843c",
    "data":{
      "api":"PutBlob",
      "requestId":"9b74f288-f01e-0028-017a-f97eac000000",
      "eTag":"0x8DCC79162A5C385",
      "contentType":"application/octet-stream",
      "contentLength":548,
      "blobType":"BlockBlob",
      "url":"https://volvop.blob.core.windows.net/devp1/MJVKZCPQ/MJVKZCPQ_20240821220411_15min_Volvo.json.gz",
      "sequencer":"0000000000000000000000000001B02000000000000ae2dc",
      "storageDiagnostics":{"batchId":"b47f65f4-c006-002b-007a-f97dab000000"}
    },
    "dataVersion":"",
    "metadataVersion":"1",
    "eventTime":"2024-08-28T18:44:01.8993801Z"
  }
} */
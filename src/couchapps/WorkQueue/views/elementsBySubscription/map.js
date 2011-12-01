function(doc, site) {
    var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];
    if (ele) {
        emit(ele["SubscriptionId"], {'_id' : doc['_id']});
    }
}
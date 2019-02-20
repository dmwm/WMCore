WMStats.namespace("LogMessage");

WMStats.LogMessage = function (couchData) {
    
    var logdbDetailData = new WMStats._StructBase();
  	
  	//set default value to []
    logdbDetailData._data =[];
    
    logdbDetailData.convertCouchData = function(data) {
                                        var dataRows = data.rows;
                                        var rows =[];
                                        for (var i in dataRows) {
                                            var tableRow = {};
                                            doc = dataRows[i].doc;
                                            tableRow.request = doc.request;
                                            tableRow.agent = doc.identifier;
                                            tableRow.thr = doc.thr;
                                            tableRow.messages = doc.messages;
                                            rows.push(tableRow);
                                        }
                                        return rows;
   	                               };
   	                               
   	logdbDetailData.setMessagesToLogDBData = function() {
   		var logDBData = WMStats.RequestLogModel.getData();
   		var d = this._data;
   		for (var i in d) {
   			logDBData.setMessages(d[i].request, d[i].agent, d[i].thr, d[i].messages);
   		}	
   	};
   	
    if (couchData) logdbDetailData.setData(couchData);
    
    logdbDetailData.setMessagesToLogDBData();
       
    return logdbDetailData;
};

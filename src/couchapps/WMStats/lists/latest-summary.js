function(head, req) {
    function andFilter(filter, data) {
        for (var key in filter) {
            //add spectial case for non existing data
            if (filter[key] == 'NoValue' && !data[key]) continue;
            if (filter[key]) {
                if (data[key] != filter[key]) { return false; }
            } 
        }
        return true;
    }
    // this function is used for elementsDetailByWorkflowAndStatus
    provides("json", function() {
        send("[");
        var row = getRow();
        var latestRecord = {};
        var commaFlag = false;
        var campaign = req.query.campaign; 
        var requestor = req.query.requestor;
        if (row) {
            if (row.key[1] == 1) { 
                send(toJSON(row.value)); commaFlag = true;
            } else {
                latestRecord = row.value;
            };
            
            while (row = getRow()){
                if (row.key[1] == 1) {
                    for (col in row.value) {
                        latestRecord[col] = row.value[col];                        
                    }
                    if (commaFlag) {send(",");}
                    send(toJSON(latestRecord));
                    latestRecord = {};
                } else {
                    latestRecord = row.value;
                }            
            }
        }// end rows
        send("]");
    })
} // end function
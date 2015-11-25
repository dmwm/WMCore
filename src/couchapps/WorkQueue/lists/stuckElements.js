function(head, req) {
    // return elements that cannot be run
    var mainDoc = this

    provides("html", function() {
        var Mustache = require("lib/mustache");
        return Mustache.to_html(mainDoc.templates.StuckElementSummary,
                                {}, mainDoc.templates.partials, send);
    });

    
    provides("json", function() {
        send("[");
        var row = getRow();
        if (row) {
        	var ele = row['doc']['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'];
        	ele.InsertTime = row['doc'].timestamp;
        	ele.UpdateTime = row['doc'].updatetime;
        	ele.reason = row['key'];
        	ele.id = row['id'];
        	send(toJSON(ele));
            while (row = getRow()) {
                //in case document is already deleted	
                if (!row.doc) {
                	continue;
                };
                send(",");
                var ele = row['doc']['WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement'];
                ele.InsertTime = row['doc'].timestamp;
                ele.UpdateTime = row['doc'].updatetime;
                ele.reason = row['key'];
                ele.id = row['id'];
                send(toJSON(ele));
            };
        }// end rows
        send("]");
    });
}; // end function
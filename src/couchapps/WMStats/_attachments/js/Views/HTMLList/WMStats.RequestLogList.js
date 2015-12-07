WMStats.namespace('WMStats.RequestLogList');
(function() { 
	
	function createRequestLogList(res, containerDiv) {
	    var pat_war = /.*warning.*/;
	    var pat_err = /.*error.*/;
	    // construct list
	    $.each(res, function(i)
	    {
	        var req = $('<div/>')
	            .addClass('request')
	            .text(res[i].request);
	        $(containerDiv).append(req);
	        var obj = res[i];
	        var keys = Object.keys(obj);
	        $.each(keys, function(j) {
	            var thr = $('<div/>')
	                .addClass('thread')
	                .text(keys[j]);
	            $(containerDiv).append(thr);
	            var values = obj[keys[j]];
	            $.each(values, function(k) {
	                var val = values[k];
	                var type = val.type;
	                var msg_cls = 'agent-info';
	                var icon_cls = '';
	                if(val.type.match(pat_war)) {
	                    msg_cls = 'agent-warning';
	                    icon_cls = 'fa fa-bell-o medium agent-warning';
	                } else if(val.type.match(pat_err)) {
	                    msg_cls = 'agent-error';
	                    icon_cls = 'fa fa-exclamation-triangle medium agent-error';
	                } else {
	                    msg_cls = 'agent-info';
	                    icon_cls = 'fa fa-check medium agent-info';
	                }
	                var icon = $('<i/>').addClass(icon_cls).addClass('type-ts-msg');
	                var date = new Date(val.ts);
	                var span = $('<span/>').addClass('type-ts-msg '+msg_cls).text(val.type);
	                var rest = $('<span/>').text(' | '+date.toGMTString()+' | '+val.msg);
	                var ttm = $('<div/>')
	                    .addClass('type-ts-msg')
	                    .add(icon).add(rest).add('<br/>');
	                $(containerDiv).append(ttm);
	            });
	        });
	        $(containerDiv).append('<br/><hr/>');
	    });
	};
    
    var errorFormat = function(logData) {
        //var formatStr = "<div class='fa fa-exclamation-triangle medium agent-error'>";
        var formatStr = "";
        for (var i in logData) {
            formatStr += "<details> <summary> <b>" + logData[i].request + "</b></summary> <ul>";
            formatStr += "<li><b>agent</b>: " + logData[i].agent + "</li>";
            formatStr += "<li><b>thread</b>: " + logData[i].thr + "</li>";
            formatStr += "<li><b>type</b>: " + logData[i].type + "</li>";
            formatStr += "<li><b>update time</b>: " + WMStats.Utils.utcClock(new Date(logData[i].ts * 1000)) + "</li>";
            formatStr += "<li><b>error message</b>: <pre>" + logData[i].messages[0].msg + "</pre></li>";
            formatStr += "<li><b>id</b>: " + logData[i].id + "</li>";
            formatStr += "</ul></details>";
        }
        //formatStr += "</div>";
        return formatStr;
    };
    
    
    var format = function (data) {
       var htmlstr = "";
       htmlstr += errorFormat(data);
       return htmlstr;
    };
    
    WMStats.RequestLogList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
    };
    
     // controller for this view to be triggered
    var vm = WMStats.ViewModel;
    vm.LogDBPage.subscribe("data", function() {
        //TODO get id form the view
        var divID = '#logdb_summary';
        logDBData = vm.LogDBPage.data();
        errors = logDBData.getLogWithLastestError();
        WMStats.RequestLogList(errors, divID);
    });
})();

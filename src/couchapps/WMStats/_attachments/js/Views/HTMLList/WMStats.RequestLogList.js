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
	            .text(res[i].key);
	        $(containerDiv).append(req);
	        var obj = res[i].value;
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
    
    WMStats.RequestLogList = function (data, containerDiv) {
        createRequestLogList(data, containerDiv);
    };
    
     // controller for this view to be triggered
    var vm = WMStats.ViewModel;
    vm.LogDBPage.subscribe("data", function() {
        //TODO get id form the view
        var divID = '#logdb_summary';
        WMStats.RequestLogList(vm.LogDBPage.data().getData(), divID);
    });
})();

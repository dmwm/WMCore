// create info list based on provided tag and result set
function create_info_list(tag, res) {
    var pat_war = /.*warning.*/;
    var pat_err = /.*error.*/;
    // construct list
    $.each(res, function(i)
    {
        var req = $('<div/>')
            .addClass('request')
            .text(res[i].key);
        $(tag).append(req);
        var obj = res[i].value;
        var keys = Object.keys(obj);
        $.each(keys, function(j) {
            var thr = $('<div/>')
                .addClass('thread')
                .text(keys[j]);
            $(tag).append(thr);
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
                $(tag).append(ttm);
            });
        });
        $(tag).append('<br/><hr/>');
    });
}

// helper functions
var not_null = function(row) {
    if(row.value!=null) return true;
    return false;
}
var ferrors = function(data) {
    var results = data.rows;
    var errors = results.filter(not_null);
    create_info_list('.request-latest-errors', errors);
}
function logdb_info(tag, request) {
    // create placeholders
    var html = '<div class="request-latest-errors"></div>';
    $(tag).html(html);
    // setup DB connection
    var db = WMStats.CouchBase('wmstats_logdb', 'wmstats_logdb');
    // define view to use
    var regex = /\,|%3C/;
    var view = 'latestErrors';
    var opts = {group:true}
    if(request==undefined || !request || request.match(regex) || request=="") { // multiple keys
        view = 'latestErrors';
        if(request!=undefined && request) {
            opts = {group:true, keys:request.split(",")};
        }
    } else { // single request
        view = 'allMessages';
        opts = {group:true, key:request};
    }
    db.view(view, opts, ferrors);
}

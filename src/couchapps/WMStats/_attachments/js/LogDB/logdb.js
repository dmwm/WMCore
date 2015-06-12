// Run the query and output data to the console.
var db = new CouchDB('wmstats_logdb');
function find_request(db, request) {
    var matches;
    matches = db.view('wmstats_logdb/lastMsgByRequest', { key: request });
    return matches.rows;
}

// helper functions
var sel_info = function(row) {
    if(row.value[0]=="agent-info") return true;
    return false;
}
var sel_error = function(row) {
    if(row.value[0]=="agent-error") return true;
    return false;
}
var sel_warning = function(row) {
    if(row.value[0]=="agent-warning") return true;
    return false;
}
var sel_other = function(row) {
    if(row.value[0]!="agent-error" && row.value[0]!="agent-info" && row.value[0]!="agent-warning") return true;
    return false;
}
function create_list(tag, res) {
    // construct list
    var cList = $('ul.' + tag)
    $.each(res, function(i)
    {
        var li = $('<li/>')
            .addClass('ui-menu-item')
            .appendTo(cList);
        var val = $('<span/>')
            .text(res[i].value[1] + " / " + res[i].value[2].msg + " / " + res[i].value[2].ts)
            .appendTo(li);
    });
}

/*
 * logdb_info will fill out the following tag element
   <div class="logdb-info"></div>
 */
function logdb_info(request) {
    // create placeholders
    var html = '<h3 class="request-header"></h3>' +
        '<h4>Info messages</h4>' +
        '<ul class="request-infos"></ul>' +
        '<h4>Error messages</h4>' +
        '<ul class="request-errors"></ul>' +
        '<h4>Warning messages</h4>' +
        '<ul class="request-warnings"></ul>' +
        '<h4>Other messages</h4>' +
        '<ul class="request-others"></ul>';
    $('div.logdb-info').html(html);
    // get data from CouchDB
    var results = find_request(db, request);
    // update request header
    $('h3.request-header').html("Request: "+request);
    // select categories
    var infos = results.filter(sel_info);
    var errors = results.filter(sel_error);
    var warnings = results.filter(sel_warning);
    var others = results.filter(sel_other);
    create_list('request-infos', infos);
    create_list('request-errors', errors);
    create_list('request-warnings', warnings);
    create_list('request-others', others);
}

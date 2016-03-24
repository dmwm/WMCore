/*
 * helper functions to fetch task data and update task parameters
 * usage:
 *     // construct the path in template, e.g.
 *     // path=$base/data/splitting/web_form/RequestName
 *     // fetch data asynchronously to update given tag
 *     ajaxRequestTaskData(path, tag);
 */
var TASK = TASK || {};
function ajaxRequestTaskData(path, tag) {
    var request = $.ajax({
        url: path,
        contentType: "application/json",
        type: 'GET',
        dataType: "json"
    });
    request.done(function(data, msg, xhr) {
	TASK.data = data.result;
	PlaceTaskData(tag);
    });
}
// helper function to construct valid href for given type/tag
function ActionTag(type, tag) {
    if (type == 'show') {
        return "<a href=\"javascript:ExpandTask('"+tag+"');\">Show</a>";
    } else {
        return "<a href=\"javascript:CollideTask('"+tag+"');\">Hide</a>";
    }
}
// helper function to expand all tasks on task web page
function ExpandAllTasks() {
    $('[name="taskName"]').show();
}
// helper function to collide all tasks on task web page
function CollideAllTasks() {
    $('[name="taskName"]').hide();
}
// helper function to expand specific task on task web page
function ExpandTask(t) {
    $('[id="'+t+'"]').show();
}
// helper function to collide specific task on task web page
function CollideTask(t) {
    $('[id="'+t+'"]').hide();
}
// helper function to collide specific task on task web page
function CollideTaskSlow(t, v) {
    $('[id="'+t+'"]').hide(v);
}
// extract algorithms for give task name
function Algs(taskName) {
    var task = FindTask(taskName);
	var params = task.splitParamList;
    var algs = [];
    for(var i=0;i<params.length; i++) {
        var p = params[i];
        for(var key in p) {
            if(key=="algorithm") {
                algs.push(p[key]);
            }
        }
    }
    return algs;
}
// extract parameters for given task and algorithm
function FindParams(taskName, alg) {
    var task = FindTask(taskName);
    var params = task.splitParamList;
    for(var i=0;i<params.length;i++) {
        if (params[i].algorithm == alg) {
            return params[i];
        }
    }
}
// helper function to switch algorithm info
function SwitchAlg(tag) {
    var alg = document.getElementById(tag).value;
    var taskName = tag.replace("__ALG__", "");
    var params = FindParams(taskName, alg);
    var t = '[id="__TABLE__'+taskName+'"]';
    $(t).replaceWith(AlgInfo(taskName, params));
}
// helper function to build algorithm info on web UI
function AlgInfo(taskName, params) {
    var html = "";
    html += '<div id="__TABLE__'+taskName+'">'
    html += '<table class="taskParams width-200">\n';
    for(var key in params) {
        if (params.hasOwnProperty(key) && key!="algorithm") {
            var v = params[key];
            if (String(v)=="false") {
                html += '<tr><td>'+key+'</td><td><select name="'+key+'"><option>false</option><option>true</option></select></td></tr>\n';
            } else if (String(v)=="true") {
                html += '<tr><td>'+key+'</td><td><select name="'+key+'"><option>true</option><option>false</option></select></td></tr>\n';
            } else {
                html += '<tr><td>'+key+'</td><td><input class="visible_input" name="'+key+'" type="text" value="'+v+'" /></td></tr>\n';
            }
        }
    }
    html += '<tr><td></td><td><button class="btn btn-small btn-green bold" onclick="javascript:SubmitTask(\''+taskName+'\')">Submit</button></td></tr>\n';
    html += '</table></div>\n';
    return html
}
// function to extract request name from task name
function requestName(taskName) {
    return taskName.split("/")[1];
}
// function to get task parameters from a form
function getTaskDict(taskName) {
    var tag = '[id="__FORM__'+taskName+'"]';
    var params = $(tag).serializeArray(); // params = [{"name":key, "value":key_value}, ...]
    var adict = {};
    var alg="";
    var splitParams = {};
    for(var i=0; i<params.length; i++) {
        var key = params[i]['name'];
        var val = params[i]['value'];
        if(key=="algorithm") {
            alg=val;
        } else {
            splitParams[key] = val;
        }
    }
    adict.splitAlgo = alg;
    adict.splitParams = splitParams;
    adict.taskName = taskName;
    return adict;
}
// helper function to submit task parameters to reqmgr2 server
function SubmitTask(taskName) {
    var data = [getTaskDict(taskName)];
    ajaxRequest('/reqmgr2/data/splitting/'+requestName(taskName), data, 'POST');
    CollideTaskSlow(taskName, 1000);
}
// helper function to submit task parameters for all tasks
function SubmitAllTasks() {
    var data = [];
    for(var i=0; i<TASK.data.length; i++) {
        var taskName = TASK.data[i].taskName;
        data.push(getTaskDict(taskName));
        CollideTaskSlow(taskName, 1000);
    }
    ajaxRequest('/reqmgr2/data/splitting/'+requestName(taskName), data, 'POST');
}
// build task params section on web UI
function TaskParams(taskName) {
    var algs = Algs(taskName);
    var html = '<div id="'+taskName+'" class="hide" name="taskName">';
    html += '<form id="__FORM__'+taskName+'" action="javascript:void(0)">';
    var atag = '__ALG__'+taskName;
	html += '<select id="'+atag+'" name="algorithm" onchange="javascript:SwitchAlg(\''+atag+'\');">';
    for(var i=0;i<algs.length;i++) {
        html += '<option value="'+algs[i]+'">'+algs[i]+'</option>';
    }
    html += '</select>'
    var params = FindParams(taskName, algs[0]);
    html += AlgInfo(taskName, params);
    html += '</form></div>';
    return html;
}
// make task entry on web UI for given task name
function MakeTask(taskName) {
    var html = '<div id="task_'+taskName+'">';
    html += ActionTag('show', taskName)+" | "+ActionTag('hide', taskName)+' &rArr; <b>'+taskName+'</b><br/>';
    html += TaskParams(taskName);
    html += "</div>";
    return html;
}
// extract task data from task data for given task name
function FindTask(taskName) {
    for(var i=0; i<TASK.data.length; i++) {
        if(TASK.data[i].taskName==taskName) {
            return TASK.data[i];
        }
    }
}
function PlaceTaskData(tag) {
    var html = '<div class="tasks shadow" id="_taskSplitting">';
    html += '<div align="right"><a href="javascript:HideTag(\'_taskSplitting\')">Close</a></div><hr/>';
    html += "<a href=\"javascript:ExpandAllTasks()\">Show All</a> | ";
    html += "<a href=\"javascript:CollideAllTasks()\">Hide All</a>";
    for(var i=0; i<TASK.data.length; i++) {
        html += MakeTask(TASK.data[i].taskName);
    }
    html += '<hr/><button class="btn btn-small btn-green right bold" onclick="javascript:SubmitAllTasks()">Submit All</button>';
    html += '<br/><br/><div align="right"><a href="javascript:HideTag(\'_taskSplitting\')">Close</a></div>';
    html += "</div>";
    var id = document.getElementById(tag);
    id.innerHTML = html;
}

// utils.js
// 
// auxiliary, common utility stuff


var utils =
{
	setUp: function()
	{
		utils.checkAndSetConsole();
	}, // setUp()

	
	// console - Firefox Firebug plugin development debug tool
	// if run without this plugin or on non-Firefox browser, it's
	// necessary to set console variable so that the code still
	// works without printing anyting to a debug console
    checkAndSetConsole: function()
    {
		if (typeof console == 'undefined') 
		{
			console = {};
			console.log = function(msg) 
			{ 
				return;
			}; 
		}
    }, // checkAndSetConsole()


	// add a main / index page link to the document body
	addPageLink: function(link, linkTitle, parentElem)
	{
		var backLink = document.createElement("div");
		backLink.innerHTML = "<a href=\"" + link + "\">" + linkTitle + "</a>";
		var divElem = document.createElement("div");
		divElem.appendChild(document.createElement("br"));
		divElem.appendChild(backLink);
		if (parentElem === null)
		{
			document.body.appendChild(divElem);			
		}
		else
		{
			document.getElementById(parentElem).appendChild(divElem);
		}				
	}, // addMainPageLink()
	
	
	// Convert a options object to an url query string.
	// ex: {key:'value',key2:'value2'} becomes '?key="value"&key2="value2"'
	encodeOptions: function(options)
	{
		var buf = [];
	    if (typeof(options) === "object" && options !== null) 
	    {
	    	for (var name in options) 
	    	{
	    		var value = options[name];
	    		if (jQuery.inArray(name, ["key", "startkey", "endkey"]) >= 0) 
	    		{
	    			value = (value !== null) ? JSON.stringify(value) : null;
	    		}
	    		buf.push(encodeURIComponent(name) + "=" + encodeURIComponent(value));
	    	}
	    }
	    return buf.length ? "?" + buf.join("&") : "";
	}, // encodeOptions()
	
	
	encodeFormData: function(data)
	{
		if (!data) return ""; // always return a string
		var pairs = [];
		for(var name in data)
		{
			if (!data.hasOwnProperty(name)) continue;
			if (typeof data[name] == "function") continue;
			var value = data[name].toString();
			name = encodeURIComponent(name).replace("%20", "+");
			value = encodeURIComponent(value).replace("%20", "+");
			pairs.push(name + "=" + value);
		}
		return pairs.join("&");
	}, // encodeFormData()

	
	// function used as XMLHttpRequest call callback
    callbackLogger: function(url, request, reloadPage)
    {
    	console.log("callbackLogger:");
    	console.log("url: " + url);
    	console.log("request status: " + request.status);
    	// at calls which modify something may be desirable to reload
    	// the page to see resources modified
    	// at a simple GET call will reloading the page here cause a loop
    	if (reloadPage)
    	{
    		window.location.reload();
    	}
    }, // callbackLogger()
    	
	
	// make XMLHttpRequest call
	// callback: function to process data, if any, on a GET call
	makeHttpRequest: function(url, callback, data, options)
	{		
		url += utils.encodeOptions(data);
		var method = options["method"];
		console.log("makeHttpRequest: " + method + " " + url);
        var request = new XMLHttpRequest();
		// open( Method, URL, Asynchronous, UserName, Password )
		// async: true (asynchronous) or false (synchronous)
        // e.g. on GET:
        // doesn't work with false: data is returned, but callback not called ...
        request.open(method, url, true);
        // without this will get JSON in curl, XML for browser or here in JS
        request.setRequestHeader("Accept", "application/json");
		request.onreadystatechange = function()
		{
			if (request.readyState === 4)
			{
				if (request.status === 200 && callback)
				{
					var json = JSON.parse(request.responseText);
					callback(json);
				}
				utils.callbackLogger(url, request, options["reloadPage"]);
			}
		};
		request.setRequestHeader("Content-Type", "application/x-www-form-urlencoded");
		if (method === "POST")
		{
			request.send(utils.encodeFormData(data));
		}
		else
		{
			request.send(null);
		}
		console.log("makeHttpRequest: request sent.");
		
	}, // makeHttpRequest()
    
    
    // this function parses ampersand-separated name-value argument pairs
    getUrlArguments: function()
    {
		var args = {};
		var query = location.search.substring(1); // without ?
		var pairs = query.split("&");
		for (var i=0; i<pairs.length; i++)
		{
			var pos = pairs[i].indexOf('=');
			if (pos == -1) continue;
			var name = pairs[i].substring(0, pos);
			var value = pairs[i].substring(pos + 1);
			value = decodeURIComponent(value);
			args[name] = value;
		}
		return args;	
    }, // getUrlArguments()
    
    
    // prints timing information to the console, e.g. how long
    // some function took
    printTiming: function(startTime, endTime, title)
    {
    	var lasted = new Date(endTime - startTime);
    	console.log(title + " took: " + lasted.getSeconds() + "s " +
    			                        lasted.getMilliseconds() + "ms");
    }
    
    	
} // utils

/*
 * ReqMgr specific utilities
 * Author: Valentin Kuznetsov
 */
function getTagValue(tag)
{
    return document.getElementById(tag).value;
}
function updateTag(tag, val) {
   var id = document.getElementById(tag);
   if (id) {
       id.value=val;
   }
}
function ClearTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        id.innerHTML="";
    }
}
function HideTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        id.className="hide";
    }
}
function ShowTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        id.className="show";
    }
}
function FlipTag(tag) {
    var id=document.getElementById(tag);
    if (id) {
        if  (id.className == "show") {
            id.className="hide";
        } else {
            id.className="show";
        }
    }
}
function ToggleTag(tag, link_tag) {
    var id=document.getElementById(tag);
    if (id) {
        if  (id.className == "show") {
            id.className="hide";
        } else {
            id.className="show";
        }
    }
    var lid=document.getElementById(link_tag);
    if (lid) {
        if  (lid.innerHTML == "show") {
            lid.innerHTML="hide";
        } else {
            lid.innerHTML="show";
        }
    }
}
function load(url) {
    window.location.href=url;
}
function reload() {
    load(window.location.href);
}
// Select all checkboxes
function CheckAll(bx) {
  var cbs = document.getElementsByTagName('input');
  for(var i=0; i < cbs.length; i++) {
    if(cbs[i].type == 'checkbox') {
      cbs[i].checked = bx.checked;
    }
  }
}
// change menu items
function ChangeMenuItem(cls, tag) {
    var items = document.getElementsByClassName(cls);
    for (var i = 0; i < items.length; i++ ) {
        items[i].className=cls;
    }
    tag.parentNode.className = cls+" active underline"
}
// Sort functions
function ByStatus(a, b) {
    if (a.RequestStatus > b.RequestStatus) {
        return 1;
    }
    if (a.RequestStatus < b.RequestStatus) {
        return -1;
    }
    return 0;
}
function ByDate(a, b) {
    if (a.RequestDate > b.RequestDate) {
        return 1;
    }
    if (a.RequestDate < b.RequestDate) {
        return -1;
    }
    return 0;
}
function ByRequestor(a, b) {
    if (a.Requestor > b.Requestor) {
        return 1;
    }
    if (a.Requestor < b.Requestor) {
        return -1;
    }
    return 0;
}
function ByGroup(a, b) {
    if (a.Group > b.Group) {
        return 1;
    }
    if (a.Group < b.Group) {
        return -1;
    }
    return 0;
}
function ByName(a, b) {
    if (a.Name > b.Name) {
        return 1;
    }
    if (a.Name < b.Name) {
        return -1;
    }
    return 0;
}
function ByCampaign(a, b) {
    if (a.Campaign > b.Campaign) {
        return 1;
    }
    if (a.Campaign < b.Campaign) {
        return -1;
    }
    return 0;
}
function ByPattern(a, b) {
    if (a.Pattern > b.Pattern) {
        return 1;
    }
    if (a.Pattern < b.Pattern) {
        return -1;
    }
    return 0;
}
function genColor(s) {
    var hash=md5(s);
    var color = '#'+hash.substr(0,6);
    return color;
}
function FilterPattern(item) {
    var input = $("#filter").val();
    var pat = new RegExp(input, "g");
    return pat.test(item.RequestName);
}
function FilterCampaign(item) {
    var input = $("#filter_campaign").val();
    var pat = new RegExp(input, "g");
    return pat.test(item.Campaign);
}

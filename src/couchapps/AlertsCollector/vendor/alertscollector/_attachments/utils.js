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
	
	
	// function returns URL of the couchapp ending with '/' after the
	// couchapp's name
	// other solutions based on counting '/' will fail in various set ups ...
	// e.g. var dbname = document.location.href.split('/')[3]; is wrong
	// below solutions relies on the always true fact there is "_design/CouchAppName"
	// and is not dependent on number of other components which may follow
	getMainUrl: function(docHref)
	{
        var split = document.location.href.split("_design/");
        var couchAppName = split[1].slice(0, split[1].indexOf('/'));
        var result = split[0] + "_design/" + couchAppName + "/";
		return result;
	}, // getMainUrl()
	

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
	// callback: function to process data, if any
	// on a GET call, it's currently only expected to be used
	// to call CouchDB so the result is tried to convert into JSON and
	// passed into the callback
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
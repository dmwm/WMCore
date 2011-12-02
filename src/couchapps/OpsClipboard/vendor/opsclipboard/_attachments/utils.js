// utils.js
// 
// auxiliary, common utility stuff


var utils =
{
		
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
	addPageLink: function(link, linkTitle)
	{
		var backLink = document.createElement("div");
		backLink.innerHTML = "<a href=\"" + link + "\">" + linkTitle + "</a>";
		document.body.appendChild(document.createElement("br"));
		document.body.appendChild(backLink);		
	} // addMainPageLink()	

} // utils
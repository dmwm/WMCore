function(doc, req) 
{
	var resp1 = "";
	resp1 = new Date();
	// multiplied by 1000 so that the argument is in milliseconds, not seconds
	var resp2 = new Date(1316096251 * 1000); // timestamp
	
    return "Test Show Function" + "  " + resp1 + "  " + resp2;
}
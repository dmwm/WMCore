function(head, req)
{	
	start({"headers": {"Content-Type": "text/html"}});
	send("<html><head><title>Test</title></head><body>\n");
	send("test list, no output");
	send("</body></html>\n");
}
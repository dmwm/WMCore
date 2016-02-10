function (doc, req) {

	if (doc.type == "fwjr" && doc.archivestatus){
		doc.archivestatus = "uploaded";
	return [doc, "OK"];
	}
}

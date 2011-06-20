function(doc, req){

    var descKey = Date.now();
    log(req.form['newdescription']);
    doc.description[descKey] = req.form.newdescription;
    return [doc, "OK"]
}
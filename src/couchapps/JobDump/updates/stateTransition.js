function (doc, req) {
  if (!doc) {
    doc = {};
    doc._id = req.id;
    doc.states = {}
  };
 
  var newTransition = {'oldstate': req.query['oldstate'],
                       'newstate': req.query['newstate'],
                       'location': req.query['location'],
                       'timestamp': parseInt(req.query['timestamp'])};

  var maxKey = 0;
  for (key in doc.states) {
    if (maxKey < parseInt(key)) {
      maxKey = parseInt(key);
    }
  }

  doc.states[(maxKey + 1) + ""] = newTransition;
  return [doc, 'OK'];
}
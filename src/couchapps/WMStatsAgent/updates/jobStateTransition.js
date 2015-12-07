function (doc, req) {
  if (!doc) {
    doc = {};
    doc._id = req.id;
    doc.state_history = new Array();
  };
  if (doc.state_history === undefined) {
      doc.state_history = new Array();
  };
  var newTransition = {'oldstate': req.query['oldstate'],
                       'newstate': req.query['newstate'],
                       'location': req.query['location'],
                       'timestamp': parseInt(req.query['timestamp'])};

  doc.state_history.push(newTransition);
  return [doc, 'OK'];
}
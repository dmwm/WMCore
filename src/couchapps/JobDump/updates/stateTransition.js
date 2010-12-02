function (doc, req) {
  var newTransition = {'oldstate': req.query['oldstate'],
                       'newstate': req.query['newstate'],
                       'location': req.query['location'],
                       'timestamp': parseInt(req.query['timestamp'])};

  doc.states.push(newTransition);
  return [doc, 'OK'];
}
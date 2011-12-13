function (doc, req) { 
  doc.state = req.query['newstate']; 
  doc.timestamp = parseInt(req.query['timestamp']); 
  return [doc, 'OK']; 
} 

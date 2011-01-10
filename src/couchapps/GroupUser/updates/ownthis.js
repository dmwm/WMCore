function (doc, req) {
  var groupName = req.query['group'];
  var userName  = req.query['user'];    
  doc.owner = {};
  doc.owner.group = groupName;
  doc.owner.user = userName;
  return [doc, 'OK'];
}
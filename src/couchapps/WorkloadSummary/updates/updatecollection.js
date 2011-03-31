function (doc, req) {
  var collName = req.query['collection'];
  doc['ACDC']["collection"] = collName;
  return [doc, 'OK'];
}
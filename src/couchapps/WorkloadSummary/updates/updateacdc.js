function (doc, req) {
  var taskName = req.query['task'];
  var filesetId = req.query['fileset'];
  doc['ACDC']['filesets'][taskName] = filesetId;
  return [doc, 'OK'];
}
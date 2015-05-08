function (doc, req) {
  if (!doc) {
    doc = {};
    doc._id = req.id;
  };
  if (doc.logArchiveLFN === undefined) {
      doc.logArchiveLFN = {};
  };
  doc.logArchiveLFN[req.query['logArchiveLFN']] = true;
  return [doc, 'OK'];
}
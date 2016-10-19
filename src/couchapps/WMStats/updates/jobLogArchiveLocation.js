function (doc, req) {
  if (!doc) {
    return [null, 'ERROR'];
  };
  if (doc.logArchiveLFN === undefined) {
      doc.logArchiveLFN = {};
  };
  doc.logArchiveLFN[req.query['logArchiveLFN']] = true;
  return [doc, 'OK'];
}
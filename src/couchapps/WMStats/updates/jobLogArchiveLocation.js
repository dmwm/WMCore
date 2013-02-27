function (doc, req) {
  if (doc.logArchiveLFN === undefined) {
      doc.logArchiveLFN = {};
  };
  doc.logArchiveLFN[req.query['logArchiveLFN']] = true;
  return [doc, 'OK'];
}
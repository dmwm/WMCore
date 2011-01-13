function (doc, req) {
  doc.states[req.query['index']]['reported'] = true;
  return [doc, 'OK'];
}
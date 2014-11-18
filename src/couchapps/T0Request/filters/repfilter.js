function(doc, req) {
  return !doc._id.match('_design/(.*)');
}
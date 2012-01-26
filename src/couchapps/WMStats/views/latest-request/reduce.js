function(keys, values, rereduce) {
  var max = values[0].timestamp;
  var max_doc = values[0];
  for (var item in values) {
    if (values[item].timestamp > max) {
      max = values[item].timestamp;
      max_doc = values[item];
    }
  }
  return {"max": max_doc};
}
function (key, values, rereduce) {
  var output = {"skippedFiles": 0};
  for (var someValue in values) {
    output['skippedFiles'] += values[someValue]['skippedFiles'];
  }
  return output;
}

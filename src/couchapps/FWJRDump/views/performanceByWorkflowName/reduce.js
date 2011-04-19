function (key, values, rereduce) {
  var output = {};
  var count  = {};

  for (var someValue in values) {
    for (var someKey in values[someValue]) {
      if (someKey == "") {
	continue;
      }
      countString = someKey + "_count";
      if (!output[someKey]) {
	output[someKey] = {};
	output[someKey]["value"] = 0;
	output[someKey]["count"] = 0;
      }
      output[someKey]["value"] += parseFloat(values[someValue][someKey]);
      output[someKey]["count"] += 1;
    }
  }//END for loop over values
  
  return output;
}
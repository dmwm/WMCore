function(keys, values) {
  var result = true;
  for (var i =0; i < values.length; i++){
      result = result && values[i];
  };
  return result;
}
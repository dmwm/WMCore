function(keys, values, rereduce) {
  var maxValue = 0;
  for (var someValue in values) {
    var intValue = parseInt(someValue)
    if (intValue > maxValue) {
      maxValue = intValue;
    }//END if loop comparing value/maxValue
  }//END for loop over values
  
  return maxValue;
}

function(key, values) {
  var output = {};
     for (var i in values) {
        var doc = values[i];
        var k   = doc['Campaign']
        var v   = doc['RequestName'];
        if (! output[k] ) {
            output[k] = new Array();
        }
        output[k][output[k].length]= v;
     }
  return output;
}

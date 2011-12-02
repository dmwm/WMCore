function (key, values, rereduce){
  var result = {};
  for (v in values){
    for (a in values[v]){
      result[a] = result[a] + values[v][a] || values[v][a];
    }
  }
  return result;
}
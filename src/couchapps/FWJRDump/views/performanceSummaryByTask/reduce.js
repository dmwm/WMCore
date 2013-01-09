function (key, values, rereduce) {

  function cloneObj(obj) {
    if (typeof obj !== "object") {
        return obj; 
    } else {
        var clonedObj = {}
        for (var prop in obj) {
            clonedObj[prop] = cloneObj(obj[prop]);
        }
        return clonedObj;
    }
  }
  
  function updateObj(baseObj, additionObj, updateFunc) {
   /*
    * update baseObj using additonObj.
    * baseObj will be updated but additonObj will the same.
    * updateFuct is the function pointer defines how the object wiil be updated
    * updateFunction takes 3 parameters, baseObj, additonObj, field
    * if udateFunc is not define use addition.
    * createFlag is set to true by default
    */
   
   for (var field in additionObj) {
        if (!baseObj[field]) {
            baseObj[field] = cloneObj(additionObj[field]);
        } else {
            if (typeof(baseObj[field]) == "object"){
                updateObj(baseObj[field], additionObj[field], updateFunc);
            } else {
                if (updateFunc instanceof Function){
                    updateFunc(baseObj, additionObj, field);
                } else {
                    //default is adding
                    baseObj[field] += additionObj[field];
                }
            }
        }
    } 
   }

  var summary = {}  
  for (var index in values) {
    updateObj(summary, values[index]);
  }
  return summary;
}

// reduce yield {thr: [{type:"type", ts:123, msg:"bla"}]}
function(keys, values) {
   var out = {};
   for(var i = 0; i < values.length; i++) {
       var thr = values[i].thr;
       if(!thr) continue;
       var obj = {type:values[i].type, ts:values[i].ts, msg:values[i].msg};
       if(thr in out) {
           out[thr].push(obj);
       } else {
           out[thr] = [obj];
       }
   }
   // reverse order of objects in thread's array
   var keys = Object.keys(out);
   for(var i=0; i<keys.length; i++) {
       out[keys[i]].reverse();
   }
   return out;
}

// reduce yield {thr: [{type:"type", ts:123, msg:"bla"}]}
function(keys, values) {
   var out = {};
   for(var i = 0; i < values.length; i++) {
       var thr = values[i].thr;
       if(!thr) continue;
       var obj = {type:values[i].type, ts:values[i].ts, msg:values[i].msg};
       if(thr in out) {
           if(out[thr].ts < values[i].ts) out[thr] = obj;
       } else {
           out[thr] = obj;
       }
   }
   // loop over output results and leave only those
   // which match given pattern
   var regex = /.*error.*|.*warning.*/;
   var keys = Object.keys(out);
   for(var i=0; i<keys.length; i++) {
       var val = out[keys[i]];
       if(!val.type.match(regex)) {
           delete out[keys[i]];
       } else {
           out[keys[i]] = [val];
       }
   }
   return out;
}

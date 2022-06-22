function(newDoc, oldDoc, userCtx) {
   // Determines the doc operation type
   var DOCOPS = { modif:0, creat:1, delet:2 };
   var docOp = oldDoc ? (newDoc._deleted === true ? DOCOPS.delet : DOCOPS.modif)
                      : DOCOPS.creat;
}

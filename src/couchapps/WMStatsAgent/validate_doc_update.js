function(newDoc, oldDoc, userCtx) {
   // Determines the doc operation type
   var DOCOPS = { modif:0, creat:1, delet:2 };
   var docOp = oldDoc ? (newDoc._deleted === true ? DOCOPS.delet : DOCOPS.modif)
                      : DOCOPS.creat;

   if (newDoc._deleted === true && !oldDoc) {
     throw({forbidden: 'Do not create deleted docs'});
   }

   var allowed = (userCtx.name === null);
   //---------------------------------
     // Throw if user not validated
   if(!allowed) {
      throw {forbidden: "User not authorized for action. only local update allowed"};
   }
};

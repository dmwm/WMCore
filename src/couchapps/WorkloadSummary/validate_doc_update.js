function(newDoc, oldDoc, userCtx) {
    // We only care if the user is someone with the correct permissions
    // there is no difference between creating a new doc or updating an old one

   if (newDoc._deleted === true && !oldDoc) {
     throw({forbidden: 'Do not create deleted docs'});
   }
}

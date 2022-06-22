function(newDoc, oldDoc, userCtx) {
    // Check permissions and filter out replication of _deleted docs

    if (newDoc._deleted === true && !oldDoc) {
      throw({forbidden: 'Do not create deleted docs'});
    }

}

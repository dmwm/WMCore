function (doc,req) {

       doc.fwjr.steps['asynStageOut1'] = {'status': 0,
                                          'location': req.query.location,
                                          'lfn': req.query.lfn};
       return [doc, "OK"];
}


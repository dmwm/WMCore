function (doc,req) {

	if (doc.type == "fwjr"){
		var checksumDic = {'adler32': req.query.adler, 'cksum': req.query.cksum};
                var asynStepFound = false
		for (var stepName in doc['fwjr']['steps']) {
                        if (stepName == "asyncStageOut1") {
				doc.fwjr.steps['asyncStageOut1']['output']['output'].push({'lfn': req.query.lfn,
                                                                                          'location': req.query.location,
                                                                                          'OutputPFN': req.query.pfn,
                                                                                          'checksums': checksumDic });
                                asynStepFound = true;
                                break;
			}
		}
                if (!asynStepFound ){
			doc.fwjr.steps['asyncStageOut1'] = {'status': 0};
			doc.fwjr.steps['asyncStageOut1']['output'] = {'output':[{'lfn': req.query.lfn,
                                                                                'location': req.query.location,									                                                                       'OutputPFN': req.query.pfn,
                   	                                                        'checksums': checksumDic }]};
                }
	return [doc, "OK"];
	}
}

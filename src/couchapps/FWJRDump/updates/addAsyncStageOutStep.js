function (doc,req) {

	if (doc.type == "fwjr"){
		var checksumDic = {'adler32': req.query.adler, 'cksum': req.query.cksum};
                var asynStepFound = false
		for (var stepName in doc['fwjr']['steps']) {
                	if (stepName == "asynStageOut1") { 
				doc.fwjr.steps['asynStageOut1']['output']['output'].push({'OutputPFN': req.query.pfn,
                                                        			          'checksums': checksumDic });	
                                asynStepFound = true;
                                break;
			}
		}
                if (!asynStepFound ){
			doc.fwjr.steps['asynStageOut1'] = {'status': 0};
			doc.fwjr.steps['asynStageOut1']['output'] = {'output':[{'OutputPFN': req.query.pfn,
                   	                                                        'checksums': checksumDic }]};
                }
	return [doc, "OK"];
	}
}

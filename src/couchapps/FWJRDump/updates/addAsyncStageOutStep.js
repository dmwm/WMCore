function (doc,req) {

	if (doc.type == "fwjr"){

		checksumDic = {'adler32': req.query.adler, 'cksum': req.query.cksum};
		doc.fwjr.steps['asynStageOut1'] = {'status': 0};
		doc.fwjr.steps['asynStageOut1']['output'] = {'output':[{'OutputPFN': req.query.pfn,
                                                                       'checksums': checksumDic }]};

	return [doc, "OK"];
	}
}

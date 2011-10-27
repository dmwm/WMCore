function (doc,req) {

	if (doc.type == "fwjr"){

		checksumDic = {'adler32': req.query.adler, 'cksum': req.query.cksum};
		doc.fwjr.steps['asyncStageOut1'] = {'status': 0};
		doc.fwjr.steps['asyncStageOut1']['output'] = {'output':[{'lfn': req.query.lfn,
                                                                       'location': req.query.location,
                                                                       'OutputPFN': req.query.pfn,
                                                                       'checksums': checksumDic }]};

	return [doc, "OK"];
	}
}

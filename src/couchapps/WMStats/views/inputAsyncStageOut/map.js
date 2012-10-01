function(doc) {
        if ( doc.type == "jobsummary" ) {
                workflow = doc['workflow'];
                job = doc['_id'];
                if (doc['state']=='success') {
			for (module in doc['output']) {
		                if (doc['output'][module]['type'] == 'output') {
			                emit(doc.timestamp, 
					{'workflow' : workflow,
			  		 'jobid' : job,
       		                         '_id': doc['output'][module]['lfn'], 
				     	 'checksums': doc['output'][module]['checksums'],
	                              	 'size': doc['output'][module]['size'], 
	                 	  	 'source' : doc['output'][module]['location']})
				}
			}
		}
	}
}

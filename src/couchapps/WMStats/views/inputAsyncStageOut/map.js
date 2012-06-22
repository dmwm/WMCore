function(doc) {
        if ( doc.type == "jobsummary" ) {
                workflow = doc['workflow'];
                job = doc['_id'];
                task = doc['task'];
                job_end_time = doc['timestamp'];
                if (doc['state']=='success') {
			for (module in doc['output']) {
		                if (doc['output'][module]['type'] == 'output') {
			                emit(doc.timestamp, 
					{'workflow' : workflow,
			  		 'jobid' : job,
   	           			 'task' : task,
       		                         '_id': doc['output'][module]['lfn'], 
				     	 'checksums': doc['output'][module]['checksums'],
	                              	 'size': doc['output'][module]['size'], 
	                 	  	 'source' : doc['output'][module]['location'],
                                         'job_end_time': job_end_time})
				}
			}
		}
	}
}

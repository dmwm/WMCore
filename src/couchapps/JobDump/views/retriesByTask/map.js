function(doc) {
        var failure_counter=-1;

        for (var state in doc['states'])  {
		// We just need to count how much times the job was created, it includes the first time.
		var newState = doc['states'][state]['newstate']
                if (newState.match(/created/)) {
     	           failure_counter++;
                }
        }
        emit([doc.workflow, doc.task, failure_counter], 1);
}

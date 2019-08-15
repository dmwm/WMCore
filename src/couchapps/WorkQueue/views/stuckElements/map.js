function(doc, site) {
	var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];

	// !code lib/workqueue_utils.js

	// list elements with known problems
	if (ele) {
		// only available elements
		if (ele.Status != 'Available') {
			return;
		}

		var stuck = false;
		var stuckMsg = "No site hosting ";

		// check for at least one site hosting input data
		var input_sites = workqueue_utils.commonInputDataSites(ele);
		if (!input_sites.length) {
			stuckMsg = stuckMsg.concat("input data; ");
			stuck = true;
		}
		// check for at least one site hosting the parent data
		var parent_sites = workqueue_utils.commonInputParentDataSites(ele);
		if (!parent_sites.length) {
			stuckMsg = stuckMsg.concat("parent data; ");
			stuck = true;
		}
		// check for at least one site hosting the pileup data
		var pileup_sites = workqueue_utils.commonPileupDataSites(ele);
		if (!pileup_sites.length) {
			stuckMsg = stuckMsg.concat("pileup data");
			stuck = true;
		}
		if (stuck) {
			emit(stuckMsg, ele["RequestName"])
		}
	}
}

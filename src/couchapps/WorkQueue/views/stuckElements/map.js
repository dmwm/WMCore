function(doc, site) {
	var ele = doc["WMCore.WorkQueue.DataStructs.WorkQueueElement.WorkQueueElement"];

	// !code lib/workqueue_utils.js

	// list elements with known problems
	if (ele) {
		// only available elements
		if (ele.Status != 'Available') {
			return;
		}

		var input_sites = workqueue_utils.commonInputDataSites(ele);
		// check for at least one site hosting input data
		if (ele.Inputs && toJSON(ele.Inputs) != '{}' && !input_sites.length) {
			emit('No site with data');
			return;
		}
		// check for one site with both inputs and parents
		var parent_sites = workqueue_utils.commonInputParentDataSites(ele);
		if (ele.Inputs && toJSON(ele.Inputs) != '{}' && ele.ParentData && toJSON(ele.ParentData) != '{}') {
			if (!parent_sites.length) {
				emit('No site with all parent data');
				return;
			}
			var parents_inputs_same_site = false;
			for (var i =0; i < input_sites.length; i++) {
				var site = input_sites[i];
				if (parent_sites.indexOf(site) > -1) {
					parents_inputs_same_site = true;
					break;
				}
			}
			if (!parents_inputs_same_site) {
				emit('No site with both input & parent data');
				return;
			}
		}
		// check for at least one site with data in the whitelist
		if (ele.SiteWhitelist.length && input_sites.length) {
			var inputs_in_whitelist = false;
			for (var i = 0; i < ele.SiteWhitelist.length; i++) {
				if (input_sites.indexOf(ele.SiteWhitelist[i]) !== -1) {
					inputs_in_whitelist = true;
					break;
				}
			}
			if (!inputs_in_whitelist) {
				emit('No hosting site in whitelist');
				return;
			}
		}
		// check for at least one site with data not in the blacklist
		if (ele.SiteBlacklist.length && input_sites.length) {
			var inputs_in_blacklist = true
			for (var i = 0; i < input_sites.length; i++) {
				if (ele.SiteBlacklist.indexOf(input_sites[i]) === -1) {
					inputs_in_blacklist = false;
					break;
				}
			}
			if (inputs_in_blacklist) {
				emit('Hosting site in blacklist');
				return;
			}
		}
	}
}

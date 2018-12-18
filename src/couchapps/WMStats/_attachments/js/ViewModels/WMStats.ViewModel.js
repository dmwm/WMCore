/** abstract environment of the web page */
WMStats.namespace("_ViewModelBase");

WMStats._ViewModelBase = function (observableObj) {
    this._eventObj = {};
    this._eventObjName = "_eventObj";
    // special observable property holding represented data
    this.data = this.observable('data', null);
    if (observableObj !== undefined) {
        this._createObservableProperty(observableObj);
    };
    this._selector = null;
    this._data = null;
};

WMStats._ViewModelBase.prototype = {

    observable: function(property, defaultValue) {
        
        var _previousValue = defaultValue;
        var _property = property;
        
        function publish(value, noTrigger) {
            if (value !== undefined) {
                _previousValue = value;
                //TODO: check the equality
                /* trigger parent object changed */
                if (!noTrigger) {
                    $(this._eventObj).triggerHandler(this._eventObjName, this);
                    /* trigger property changed */
                    $(this._eventObj).triggerHandler(_property, value);
                
                }
                return this;
            } else {
                return _previousValue;
            }
        }
        return publish;
    },
    
    subscribe: function() {
        /*
         * takes 1 or 2 arguments 
         * if 1, it is callback function when this object changes
         * if 2. 1st is property name and 2nd is callback function
         * 
         */
        var callback;
        var eventName;
        if (arguments.length == 2) {
            callback = arguments[1];
            eventName = arguments[0];
        } else if (arguments.length == 1) {
            callback = arguments[0];
            eventName = this._eventObjName;
        }
        
        $(this._eventObj).on(eventName, callback);
    },
    
    id: function(selector) {
        // link the selector with the ViewModel
        // may need to connect multiple selector
        if (selector) {
            this._selector = selector;
            WMStats.ViewModel.Registry[selector] = this;
            return this;
        } else {
            return this._selector;
        } 
    },
    
    _createObservableProperty: function(observableObj) {
        for (var prop in observableObj) {
            this[prop] = this.observable(prop, observableObj[prop]);
        }
    }
};

WMStats.namespace("ViewModel");

WMStats.ViewModel = (function (){
    var properties = {page: null};
    var vm = new WMStats._ViewModelBase(properties);
    vm.propagateUpdate = function() {
        if (vm.page().propagateUpdate) {
            vm.page().propagateUpdate();
        }
     };
    return vm;
})();

// create WMStats.ViewModel properties
(function(vm) {
    
    vm.Registry  ={};

    vm.ActiveRequestPage = (function (){
        var properties = {view: null,
                          filter: {},
                          filteredStats: null,
                          refreshCount: 0};
        var requestPage = new WMStats._ViewModelBase(properties);
        
        requestPage.propagateUpdate = function() {
            var requestData = WMStats.ActiveRequestModel.getData();
            if (requestData === null) {
                return false;
            } else {
                var filter = vm.ActiveRequestPage.filter();
                vm.ActiveRequestPage.data(requestData.filterRequests(filter));
                if (requestPage.view().propagateUpdate) {
                    requestPage.view().propagateUpdate();
                }
                return true;
            }
        };
        
        return requestPage;
    })();
    
    vm.RequestAlertPage = (function (){
        var alertPage = new WMStats._ViewModelBase();
        
        alertPage.propagateUpdate = function() {
            var requestData = WMStats.ActiveRequestModel.getData();
            if (requestData === null) {
                return false;
            } else {
                vm.RequestAlertPage.data(requestData);
                return true;
            }
        };
        
        return alertPage;
    })();
    
    vm.AgentPage = (function (){
        var agentPage =new WMStats._ViewModelBase();
        
        agentPage.propagateUpdate = function() {
            var agentData = WMStats.AgentModel.getData();
            if (agentData === null) {
                return false;
            } else {
                vm.AgentPage.data(agentData);
                return true;
            }
        };
        return agentPage;
    })();
    
    vm.LogDBPage = (function (){
        var logDBPage =new WMStats._ViewModelBase();
        
        logDBPage.propagateUpdate = function() {
        
            var logDBData = WMStats.RequestLogModel.getData();
            if (logDBData === null) {
                return false;
            } else {
                vm.LogDBPage.data(logDBData);
                return true;
            }
        };
        return logDBPage;
    })();
    
    vm.SearchPage = (function (){
        /*
         * keys contain {searchCategory: blah, searchValue: blah}
         */
        var properties = {keys: null};
        searchPage = new WMStats._ViewModelBase(properties);
        
        searchPage.retrieveData = function(keys) {
            var selectedSearch = keys.searchCategory;
            var searchStr = keys.searchValue;
            var view;
            var options =  {'include_docs': true, 'reduce': false};
            if (selectedSearch === 'request') {
                view = "allDocs";
                options.key = searchStr;
            } else if (selectedSearch === 'outputdataset') {
                view = "byoutputdataset";
                options.key = searchStr;
            } else if (selectedSearch === 'inputdataset') {
                view = "byinputdataset";
                options.key = searchStr;
            } else if (selectedSearch === 'prep_id') {
                view = "byprepid";
                options.key = searchStr;
            } else if (selectedSearch === 'data_pileup') {
                view = "bydatapileup";
                options.key = searchStr;
            } else if (selectedSearch === 'mc_pileup') {
                view = "bymcpileup";
                options.key = searchStr;
            } else if (selectedSearch === 'request_date') {
                view = "bydate";
                var beginDate = $('input[name="dateRange1"]').val().split("/");
                var endDate = $('input[name="dateRange2"]').val().split("/");
                options.startkey = [Number(beginDate[0]), Number(beginDate[1]), Number(beginDate[2])];
                options.endkey = [Number(endDate[0]), Number(endDate[1]), Number(endDate[2]), {}];
            };
            
            WMStats.RequestSearchModel.retrieveData(view, options);
        };
        return searchPage;
    })();
    
    vm.CategoryView = (function (){
        
        var properties = {category: null,
                          detailView: null};
                          
        var categoryView = new WMStats._ViewModelBase(properties);
        
        categoryView.convertToCategoryData = function (requestData) {
            if (requestData === undefined) {
                requestData = vm.ActiveRequestPage.data();
            }
            var category = this.category().name();
            var summaryStruct = WMStats.CategorySummaryMap.get(this.category().name());
            var categoryData = WMStats.RequestsByKey(category, summaryStruct);
            categoryData.categorize(requestData);
            return categoryData;
        };
        
        categoryView.propagateUpdate = function() {
            if (vm.ActiveRequestPage.data() === null) {
                return false;
            };
            categoryView.data(categoryView.convertToCategoryData());
            return true;
        };
        
        return categoryView;
    })();
    
    vm.RequestView = (function (){
        /*
         * format is progress or numJobs - this is tied to name of the button
         * un tie.
         */
        
        var properties = {categoryKey: "all", 
                          format: null, 
                          detailView: null};
        
        var requestView = new WMStats._ViewModelBase(properties);
        
        requestView.propagateUpdate = function() {
            if (vm.ActiveRequestPage.data() === null) {
                return false;
            };
            if (requestView.categoryKey() == "all") {
                requestView.data(vm.ActiveRequestPage.data());
            } else {
                var categoryData = vm.CategoryView.convertToCategoryData();
                var data = categoryData.getRequestData(requestView.categoryKey());
                requestView.data(data);
            };
            return true;
        };
        //TODO: hack for assigning for category map
        requestView.categoryName = "requests";
        return requestView;
    })();
    
    vm.TaskView = (function (){
        /*
         * TODO: not done need to replace in TaskSummary Table.
         */
        
        var properties = {requestName: null};
        
        var taskView = new WMStats._ViewModelBase(properties);
        
        taskView.propagateUpdate = function() {
            if (taskView.requestName()) {
                taskView.data(WMStats.ActiveRequestModel.getData().getTasks(taskView.requestName()));
                return false;
            } else {
                return false;
            }
        };
        return taskView;
    })();
    
    function createJobSummaryView(){
        var properties = {requestName: null, detail: null};
        var jobView = new WMStats._ViewModelBase(properties);
        
        jobView.retrieveData = function(requestName) {
            WMStats.JobSummaryModel.setRequest(requestName);
            WMStats.JobSummaryModel.retrieveData();
        };
        
        jobView.propagateUpdate = function() {
            
            /* this part is needed if you want to refresh the job view when data is updated */
            if (jobView.requestName()) {
                jobView.retrieveData(jobView.requestName());
                return false;
            } else {
                return false;
            }
        };
        
        jobView.updateDataAndChild = function(data) {
            if (data) {jobView.data(data);}
            if (jobView.detail().propagateUpdate) {
                jobView.detail().propagateUpdate();
                return true;
            }
            return false;
        };
        
        return jobView;
    };
    
    vm.JobView = createJobSummaryView();
    vm.AlertJobView = createJobSummaryView();
    
    vm.CampaignCategory = (function (){
        return new WMStats._ViewModelBase({name: "campaign"});
    })();
    
    vm.SiteCategory = (function (){
        return new WMStats._ViewModelBase({name: "sites"});
    })();
    
    
    vm.CMSSWCategory = (function (){
        return new WMStats._ViewModelBase({name: "cmssw"});
    })();
    
    vm.AgentCategory = (function (){
        return new WMStats._ViewModelBase({name: "agent"});
    })();
    
    vm.RunCategory = (function (){
        return new WMStats._ViewModelBase({name: "run"});
    })();
    
    /* request view summary format */
    vm.RequestProgress = (function (){
        return new WMStats._ViewModelBase({name: "progress"});
    })();
    
    vm.RequestJobs = (function (){
        return new WMStats._ViewModelBase({name: "numJobs"});
    })();
    
    /* request view job detail */
    vm.CategoryDetail = (function (){
        var properties = {categoryKey: null};
        var categoryDetail = new WMStats._ViewModelBase(properties);
        
        categoryDetail.propagateUpdate = function() {
            if (categoryDetail.categoryKey()) {
                var allData = vm.CategoryView.data();
                categoryDetail.data(allData.getData(categoryDetail.categoryKey()));
            }
        };
        return categoryDetail;
    })();
    
   
    vm.RequestDetail = (function (){
        var properties = {requestName: null};
        var requestDetail = new WMStats._ViewModelBase(properties);
        requestDetail.open = false;
        return requestDetail;
    })();
    
    function createJobDetailView(){
        var properties = {keys: null, indexID: null};
        var jobDetail = new WMStats._ViewModelBase(properties);
        
        jobDetail.retrieveData = function(keys) {
            WMStats.JobDetailModel.setOptions(keys);
            WMStats.JobDetailModel.retrieveData();
        };
        
        jobDetail.propagateUpdate = function() {
            /* this part is needed if you want to refresh the job view when data is updated */
            if (jobDetail.keys() !== null) {
                jobDetail.retrieveData(jobDetail.keys());
                return true;
            } else {
                return false;
            }
        };
        
        return jobDetail;
    };
    
    vm.JobDetail = createJobDetailView();
    vm.AlertJobDetail = createJobDetailView();
    
    vm.Resubmission = (function (){
        /* resubmission keys contains 
         * requestName and taskName and acdc url
         * {requestName: blah, task: blah, acdcURL: blah}
         * */
       
        var properties = {keys: null};
        var resubmission = new WMStats._ViewModelBase(properties);
        
        resubmission.retrieveData = function(keys) {
            WMStats.ReqMgrRequestModel.retrieveDoc(keys.requestName);
        };
        
        resubmission.formatResubmissionData = function(reqMgrRequest) {
            var summary = {};
            // from resubmission keys
            summary.RequestType = "Resubmission";
            summary.OriginalRequestName = resubmission.keys().requestName;
            summary.InitialTaskPath = resubmission.keys().task;
            if (resubmission.keys().acdcURL) {
                //TODO: if there is not acdc_url don't create the button'
                var acdcServiceUrl = WMStats.Utils.splitCouchServiceURL(resubmission.keys().acdcURL);
                summary.ACDCServer = acdcServiceUrl.couchUrl;
                summary.ACDCDatabase = acdcServiceUrl.couchdb;
            }
            
            var requestInfo = reqMgrRequest.getData();
            // passed to wmstats view
            summary.RequestString = WMStats.Utils.acdcRequestSting(summary.OriginalRequestName, requestInfo.Requestor);
            summary.Campaign = requestInfo.Campaign;
            summary.RequestPriority = requestInfo.RequestPriority;

            //TODO get the site white list from acdc db. (site might be overflowed)
            // assign more value for acdc assignment
            summary.Team = requestInfo.Team;
            summary.TrustPUSitelists = false;
            summary.TrustSitelists = false;

            return summary;
        };
        
        resubmission.propagateUpdate = function() {
            if (resubmission.keys() !== null) {
                resubmission.retrieveData(resubmission.keys());
                return true;
            } else {
                return false;
            }
        };
        
        return resubmission;
    })();

    vm.initialize = (function (){ 
        //default setting
        vm.page(vm.ActiveRequestPage, true);
        vm.ActiveRequestPage.view(vm.CategoryView, true);
        vm.CategoryView.category(vm.CampaignCategory, true);
        vm.RequestView.format(vm.RequestProgress, true);
        vm.JobView.detail(vm.JobDetail, true);
    })();

})(WMStats.ViewModel);

// control inside the view model
(function(vm){
     // filter control
    vm.ActiveRequestPage.subscribe("filter", function() {
            vm.ActiveRequestPage.propagateUpdate();
        });
    
    vm.SearchPage.subscribe("keys", function(){
        vm.SearchPage.retrieveData(vm.SearchPage.keys());
    });
    
    vm.CategoryView.subscribe("category", function() {
        vm.CategoryView.propagateUpdate();
    });
    
    vm.RequestView.subscribe("categoryKey", function() {
        vm.ActiveRequestPage.view(vm.RequestView);
    });
    
    vm.JobView.subscribe("requestName", function() {
        vm.ActiveRequestPage.view(vm.JobView);
    });
    
    vm.AlertJobView.subscribe("requestName", function() {
        vm.AlertJobView.retrieveData(vm.AlertJobView.requestName());
    });
    
    vm.CategoryDetail.subscribe("categoryKey", function() {
        vm.CategoryDetail.propagateUpdate();
    });
    
    vm.JobDetail.subscribe("keys", function() {
        vm.JobDetail.retrieveData(vm.JobDetail.keys());
    });
    
    vm.AlertJobDetail.subscribe("keys", function() {
        vm.AlertJobDetail.retrieveData(vm.AlertJobDetail.keys());
    });
    
    vm.Resubmission.subscribe("keys", function() {
        vm.Resubmission.retrieveData(vm.Resubmission.keys());
    });

})(WMStats.ViewModel);

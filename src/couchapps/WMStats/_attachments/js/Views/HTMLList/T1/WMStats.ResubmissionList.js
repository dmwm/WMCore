WMStats.namespace('ResubmissionList');
(function() { 
    var format = function (summary) {
        var htmlstr = "";
        htmlstr += "<legend>Resubmission</legend>";
        htmlstr = '<div class="closingButton">X</div>';
        htmlstr += "<div class='requestDetailBox'>";
        htmlstr += "<ul>";
        htmlstr += "<li><b>Request String: </b><input type='text' name='RequestString' size=70 value='" + summary.RequestString + "'/></li>";
        htmlstr += "<li><b>Original Request Name:</b> " + summary.OriginalRequestName + "</li>";
        htmlstr += "<li><b>Campaign:</b> " + summary.Campaign + "</li>";
        htmlstr += "<li><b>Initial Task Path:</b> " + summary.InitialTaskPath + "</li>";

        if (summary.ACDCServer) {
            htmlstr += "<li><b>ACDC Server URL:</b> " + summary.ACDCServer + "</li>";
            htmlstr += "<li><b>ACDC DB Name:</b> " + summary.ACDCDatabase + "</li>";
        } else {
            htmlstr += "<li><b>ACDC Server URL:</b><input type='text' name='ACDCServer' size=30 /></li>";
            htmlstr += "<li><b>ACDC DB Name:</b><input type='text' name='ACDCDatabase' size=30  value='wmagent_acdc'/></li>";
        }
        htmlstr += "<li><b>RequestPriority:</b> " + summary.RequestPriority + "</li>";
        /*
        htmlstr += "<li><b>Ignored Output Modules:</b> <input type='text' name='ACDCDatabase' size=30/>";
        htmlstr += "<li><b>Alternative Collection Name:</b><input type='text' name='CollectionName' size=40/>";
        */
        htmlstr += "</ul>";
        htmlstr += "<button type='submit' value=test'> submit</button>";
        htmlstr += "</div>";
        
        
        return htmlstr;
    };
    
    WMStats.ResubmissionList = function (data, containerDiv) {
         $(containerDiv).html(format(data));
         $(containerDiv).show("slide", {}, 500);
    };
    
    var vm = WMStats.ViewModel;
    vm.Resubmission.subscribe("data", function() {
        WMStats.ResubmissionList(vm.Resubmission.data(), vm.Resubmission.id());
    });

    $(document).on('click', "#acdc_submission button", function(event){
        var resubmissionData = WMStats.ViewModel.Resubmission.data();
        resubmissionData.RequestString = $('#acdc_submission input[name="RequestString"]').val();
        if (resubmissionData.ACDCServer === undefined) {
            resubmissionData.ACDCServer = $('#acdc_submission input[name="ACDCServer"]').val();
        }
        if (resubmissionData.ACDCDatabase === undefined) {
            resubmissionData.ACDCDatabase = $('#acdc_submission input[name="ACDCDatabase"]').val();
        }
        //WMStats.ViewModel.Resubmission.Requestor = $('#acdc_submission input[name="Requestor"]').val()
        WMStats.Ajax.requestMgr.putRequest(resubmissionData);
        event.preventDefault();
       });
     
     $(WMStats.Globals.Event).on(WMStats.CustomEvents.RESUBMISSION_SUCCESS, 
        function(event, reqInfo) {
            $('#acdc_submission div.requestDetailBox').append(WMStats.Utils.formatReqDetailUrl(reqInfo.name, 
            	                                              reqInfo.reqmgr2Only));
    });

})();
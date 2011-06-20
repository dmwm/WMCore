//
// js library for drawing the ops clipboard interface to handle editing a single
// request
//
var opsclipboard = {

    // some global variables, couch interface & couch doc id
    couchdb : null,
    documentId : null, 
    mainpage : null, 

    // util to bootstrap the couch API
    setCouchDB: function(){
        var dbname = document.location.href.split('/')[3];
        console.log("couchdb ref set...")
        this.couchdb = $.couch.db(dbname);
        this.mainpage = this.couchdb.uri + "_design/OpsClipboard/index.html";
    },
    

    // submit the description form to the update handler
    // called when the add description button is clicked
    submitNewDesc : function(){
        console.log("submitNewDesc");
        var form = document.getElementById("descriptionForm")
        var adddesc = opsclipboard.couchdb.uri + "_design/OpsClipboard/_update/adddescription/" + opsclipboard.documentId;
        console.log(adddesc);
        console.log(form.newDescription.value);
        $.post(adddesc, {"newdescription" : form.newDescription.value},
            function(data){console.log(data);}
        );
        window.location.reload();
       
    },
    
    //
    // submit the change state form to the update handler when the
    // Change button is called. 
    // 
    submitNewState : function(){
        // submit the state change form to the update handler
        var form = document.getElementById("stateForm");
        var changeState = opsclipboard.couchdb.uri + "_design/OpsClipboard/_update/changestate/" + opsclipboard.documentId;

        if (form.selectState.value == ""){
            return;
        } else {
            $.post(changeState, {"selectState" : form.selectState.value},
                    function(data){console.log(data);}
                );
            window.location.reload();
        }
    },
    
    //
    // main function that draws the page elements and generates the forms
    // 
    clipboardview: function(docId, clipElem){
        this.documentId = docId;
        console.log("clipboardview");
        clipElem.style.width = "800px";
        clipElem.style.height = "600px";
        
        // build the description display form on the right
        // and the details table display on the left
        var tablePanel = document.createElement("div");
        tablePanel.style.width = "400px";
        tablePanel.style.height = "600px";
        tablePanel.style.float = "left";
        tablePanel.id = "tablePanel";
        clipElem.appendChild(tablePanel);
        
        // building the description display and update forms
        var descPanel = document.createElement("div");
        descPanel.style.width = "400px";
        descPanel.style.height = "600px";
        descPanel.style.float = "left";
        clipElem.appendChild(descPanel);
        
        // description form submits itself to the adddescription couch update via a POST
        var descriptionForm = document.createElement("form")
        descriptionForm.method = "POST";
        descriptionForm.action = opsclipboard.couchdb.uri + "_design/OpsClipboard/_update/adddescription/" + opsclipboard.documentId;
        descriptionForm.id = "descriptionForm";
        descriptionForm.name = "descriptionForm";
        descPanel.appendChild(descriptionForm);
        
        // display textarea for current comments
        var desc = document.createElement('textarea');
        desc.style.width = "400px";
        desc.style.height = "450px";
        desc.style.float = "top";
        desc.id = "descriptionDisplay";
        desc.name = "currentdescription";
        descriptionForm.appendChild(desc);
        
        // text area for entering a new description
        var newdesc = document.createElement("textarea");
        newdesc.style.width  = "400px";
        newdesc.style.height = "100px";
        newdesc.style.float = "top";
        newdesc.value = "Add a note here..."
        newdesc.id = "newDescription";
        newdesc.name = "newdescription";
        // auto highlight the initial text to make it easy to overwrite
        newdesc.onclick = function(){newdesc.select();}
        descriptionForm.appendChild(newdesc);
        
        // button to trigger form post
        var submitdesc = document.createElement("input");
        submitdesc.type = "button";
        submitdesc.value="Add Description";
        submitdesc.onclick = this.submitNewDesc;
        descriptionForm.appendChild(submitdesc);
        
        // build the state display on the left
        // state form submits itself to the couch changestate update using a POST
        var stateForm = document.createElement("form");
        stateForm.method = "POST";
        stateForm.action = opsclipboard.couchdb.uri + "_design/OpsClipboard/_update/changestate/" + opsclipboard.documentId;
        stateForm.id = "stateForm";
        stateForm.name = "stateForm";
        stateForm.width = "400px";
        stateForm.height = "150px";
        stateForm.float = "top";
        tablePanel.appendChild(stateForm);

        // display the current state in a labelled div
        var stateLabel = document.createElement("label");
        stateLabel.for = "currState";
        stateLabel.innerHTML = "<p>Current State:</p>";
        var currentState = document.createElement("div")
        currentState.id = "currState";
        currentState.innerHTML = "<p>Current State</p>";
        currentState.width = "400px";
        currentState.height = "100px";
        currentState.float = "top";
        stateForm.appendChild(stateLabel);
        stateForm.appendChild(currentState);
        
        // generate a menu of possible states that the current state can be advanced to
        var menuLabel = document.createElement("label");
        menuLabel.for = "selectState";
        menuLabel.innerHTML = "<p>Change State:</p>";
        
        var stateMenu = document.createElement("select");
        stateMenu.id = "selectState";
        stateMenu.name = "selectState";
        stateMenu.width = "400px";
        stateMenu.height = "100px";
        
        
        stateForm.appendChild(menuLabel);
        stateForm.appendChild(stateMenu);

        // button to submit the change of state form
        var submitState = document.createElement("input");
        submitState.type = "button";
        submitState.value="Change";
        submitState.onclick = this.submitNewState;
        stateForm.appendChild(submitState);
        
        // request information table
        var reqPanel = document.createElement("div");
        reqPanel.width = "400px";
        reqPanel.height = "300px";
        reqPanel.float = "top";
        reqPanel.innerHTML = "<p>Request Details</p>";
        tablePanel.appendChild(reqPanel);
        
        // main page link
        var backLink = document.createElement("div");
        backLink.innerHTML = "<a href=\"" + opsclipboard.mainpage + "\">Main Page</a>";
        document.body.appendChild(backLink);
        
    },
        
    requestDetail : function(key, value){
        
        var hozlist = document.createElement("ul");
        var elem1 = document.createElement("li");
        elem1.innerHTML = key;
        elem1.style.display = "inline";
        elem1.style.listStyleType =  "none";
        elem1.style.paddingRight = "2px";
        var elem2 = document.createElement("li");
        elem2.innerHTML = value;
        elem2.style.display = "inline";
        elem2.style.listStyleType =  "none";
        elem2.style.paddingRight = "2px";
        hozlist.appendChild(elem1);
        hozlist.appendChild(elem2);
        return hozlist;
    },
        
    populateRequestDetails : function(details){
        var tblPanel = document.getElementById("tablePanel");
        for ( i in details){
            tblPanel.appendChild(opsclipboard.requestDetail(i, details[i]));
        }
    },
        
    // populate the state change menu with the list of potential states
    populatePotentialStates : function(statesList){
        var stateMenu = document.getElementById("selectState");
        for (x in statesList){
            stateMenu.options[x] = new Option(statesList[x], statesList[x], true, false);
        } 
    },
    
    // populate the current state div with the state provided
    populateCurrentState : function(state) {
        var stateDiv = document.getElementById("currState");
        stateDiv.innerHTML = "<p>"+ state + "</p>";
    },
    
    // populate the description display with the current list of time sorted descriptions
    populateExistingDescs : function(descs){
        var descriptions = "Descriptions:\n";
        for (x in descs){
            var t = parseInt(x);
            var d = new Date(t);
            descriptions += "\n" + d.toLocaleString() + ":\n";
            descriptions += descs[x] + "\n";
        }
        var descdisplay = document.getElementById("descriptionDisplay");
        descdisplay.value = descriptions;
    },
        
    // update funtion
    // retrieve the latest version of the doc from couch and populate the displays and form
    // fields 
    update : function(){          
        this.couchdb.openDoc(opsclipboard.documentId ,{
              success : function(data){
                  opsclipboard.populateExistingDescs(data.description);
                  opsclipboard.populateCurrentState(data.state);
                  opsclipboard.populatePotentialStates(opsstates.states[data.state]);
                  opsclipboard.populateRequestDetails(data.request);
              }
          })
        
    },
    
}
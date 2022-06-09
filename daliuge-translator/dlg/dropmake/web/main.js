$(document).ready(function () {
    // jquery starts here
    //hides the dropdown navbar elements when stopping hovering over the element
    $(".dropdown-menu").mouseleave(function () {
        $(".dropdown-menu").dropdown('hide')
    })

    //export physical graph button listener
    $("#Pysical_graph").click(function () {
        $("#gen_pg_button").val("Generate Physical Graph")
        $("#dlg_mgr_deploy").prop("checked", false)
        $("#pg_form").submit();
    })

    //get saved settings from local storage or set a default value
    fillOutSettings()

    $('#settingsModal').on('hidden.bs.modal', function () {
        fillOutSettings()
    });

    updateDeployOptionsDropdown()

    $("#aboutModal #aboutLicense").load("/static/license.html")

    //keyboard shortcuts
    var keyboardShortcuts = []
    keyboardShortcuts.push({name:"Open Settings", shortcut:"O", code:79, action: "$('#settingsModal').modal('toggle')"})
    keyboardShortcuts.push({name:"Deploy", shortcut:"D", code:75, action: "$('#shortcutsModal').modal('toggle')"})
    keyboardShortcuts.push({name:"Open Keyboardshortcuts Modal", shortcut:"K", code:68, action: "$('#activeDeployMethodButton').click()"})

    //fill out keyboard shortcuts modal
    keyboardShortcuts.forEach(element => { 
        var shortCutItem =   '<div class="col-lg-6">'+
                            '<div class="shortCutsModalItem">'+
                                '<span>'+element.name+'</span>'+
                                '<span class="shortCutsModalItemRight">'+element.shortcut+'</span>'+
                            '</div>'+
                        '</div>'
        $("#shortcutsModal .modal-body .row").append(shortCutItem)
    })

    //keyboard shortcuts execution
    $(document).keydown(function(e){
        if($("input").is(":focus")){
            return
        }
        keyboardShortcuts.forEach(element => { 
            
            if (e.which == element.code) //open settings modal on o
            {
                eval(element.action)
            }
        })
    })
});

function openSettingsModal(){
    //needed for the dropdown option to open the settings modal, the pure bootstrap method used on the settings gear button proved inconsistent
    $('#settingsModal').modal("show")
}

async function initiateDeploy(method, selected, name){
    if (selected === false){
        changeSelectedDeployMethod(name)
    }
    var activeUrlReachable = await checkUrlStatus(window.localStorage.getItem("manager_url"))

    if(!activeUrlReachable){
        $("#warning-alert").fadeTo(2000, 1000).slideUp(200, function() {
            $("#warning-alert").slideUp(200);
        });
        return
    }
    if(method === "direct"){
        $("#gen_pg_button").val("Generate &amp; Deploy Physical Graph")
        $("#dlg_mgr_deploy").prop("checked", true)
        $("#pg_form").submit();
    }else if(method === "helm"){
        $("#gen_helm_button").val("Generate &amp; Deploy Physical Graph")
        $("#dlg_helm_deploy").prop("checked", true)
        $("#pg_helm_form").submit()
    }else if(method === "rest"){
        restDeploy()
    }
}

function changeSelectedDeployMethod(name) {
    var deployMethodsArray = JSON.parse(localStorage.getItem("deployMethods"))
    deployMethodsArray.forEach(element => {
        element.active = "false"
        if(element.name === name){
            element.active = "true"
        }
    })
    localStorage.setItem('deployMethods', JSON.stringify(deployMethodsArray))
    updateDeployOptionsDropdown()
}

function updateDeployOptionsDropdown() {
    //remove old options
    $(".deployMethodMenuItem").remove()
    var selectedUrl

    //add deployment options
    JSON.parse(localStorage.getItem("deployMethods")).forEach(element => {
        if(element.active === "false"){
            //dropdown options
            $("#deployDropdowns .dropdown-menu").prepend(
                `<a href='javascript:void(0)' onclick='initiateDeploy("`+element.deployMethod+`",false,"`+element.name+`")' class='dropdown-item tooltip tooltipLeft deployMethodMenuItem' data-text='Deploy Physical Graph via method: `+element.deployMethod+`' value='Deploy Physical Graph via `+element.deployMethod+`'>`+element.name+`</a>`
            )
        }else {
            selectedUrl=element.url
            //active option
            $("#deployDropdowns").prepend(
                `<a href='javascript:void(0)' id='activeDeployMethodButton'  onclick='initiateDeploy("`+element.deployMethod+`",true,"`+element.name+`")' class='dropdown-item tooltip tooltipLeft deployMethodMenuItem' data-text='Deploy Physical Graph vi method: `+element.deployMethod+` [D]' value='Deploy Physical Graph via `+element.deployMethod+`'>Deploy: `+element.name+`</a>`
            )
            checkActiveDeployMethod(selectedUrl)
        }
    })

    var newUrl = new URL(selectedUrl);
    var newPort = newUrl.port;
    var newHost = newUrl.hostname;
    var newPrefix = newUrl.pathname;
    var newProtocol = newUrl.protocol;
    console.log("URL set to:'" + newUrl + "'"); 
    console.log("Protocol set to:'" + newProtocol + "'");
    console.log("Host set to:'" + newHost + "'");
    console.log("Port set to:'" + newPort + "'");
    console.log("Prefix set to:'" + newPrefix + "'");

    window.localStorage.setItem("manager_url", newUrl);
    window.localStorage.setItem("manager_protocol", newProtocol);
    window.localStorage.setItem("manager_host", newHost);
    window.localStorage.setItem("manager_port", newPort);
    window.localStorage.setItem("manager_prefix", newPrefix);

}

async function checkUrlStatus (url) {
    return new Promise((resolve, reject) => {
        $.ajax({url: url,
            type: 'HEAD',
            dataType: 'jsonp',
            complete: function(jqXHR, textStatus){
                if(jqXHR.status === 200){
                    resolve(true) 
                }else{
                    resolve(false) 
                }
            },
            timeout: 2000
        });
    })
}

async function manualCheckUrlStatus (clickPos) {
    var badUrl = false
    //if the event is triggered by click the check icon manually
    if(clickPos === "icon"){
        var url = $(event.target).parent().find($('.deployMethodUrl')).val()
        var target = $(event.target)
    }else{
        //if the event is triggered by focus leave on the input
        var url = $(event.target).parent().parent().find($('.deployMethodUrl')).val()
        var target = $(event.target).parent().parent().find($('.urlStatusIcon'))
    }
   

    //check if jquery deems the url constructed properly
    try {
        new URL(url);
    } catch (error) {
        badUrl = true
    }

    if(badUrl){
        $("#settingsModalErrorMessage").html('Please ensure deploy methods URLs are valid')
        return
    }else{
        //if the url is deemed contructed well, here we use an ajax call to see if the url is reachable
        var urlStatus = await checkUrlStatus(url)
        $("#settingsModalErrorMessage").html('')
        if(urlStatus){
            target.replaceWith(`<div class="settingsInputTooltip tooltip tooltipBottom urlStatusIcon urlStatusReachable" data-text="Destination URL Is Reachable, click to re-check" onclick="manualCheckUrlStatus('icon')"><i class="material-icons md-24">done</i></div>`)
        }else{
            target.replaceWith(`<div class="settingsInputTooltip tooltip tooltipBottom urlStatusIcon urlStatusNotReachable" data-text="Destination URL Is Not Reachable, click to re-check" onclick="manualCheckUrlStatus('icon')"><i class="material-icons md-24">close</i></div>`)
        }   
    }
}

async function checkActiveDeployMethod(url){
    $("#activeDeployMethodButton").removeClass("activeDeployMethodButtonOnline")
    $("#activeDeployMethodButton").removeClass("activeDeployMethodButtonOffline")
    var urlStatus = await checkUrlStatus(url)
    if(urlStatus){
        $("#activeDeployMethodButton").addClass("activeDeployMethodButtonOnline")
    }else{
        $("#activeDeployMethodButton").addClass("activeDeployMethodButtonOffline")
    }   
}

function saveSettings() {
    //need a check function to confirm settings have been filled out correctly

    var settingsDeployMethods = $("#DeployMethodManager .input-group")//deploy method rows selector
    var deployMethodsArray = []//temp array of deploy method rows values
    
    //errors
    var errorFillingOut = false
    var duplicateName = false
    var emptyName = false
    var badUrl = false

    settingsDeployMethods.each(function(){

        //error detection
        if($(this).find(".deployMethodName").val().trim() === ""){
            emptyName = true
        }

        try {
            new URL($(this).find(".deployMethodUrl").val());
          } catch (error) {
                console.log("faulty Url: ",$(this).find(".deployMethodUrl").val())
              badUrl = true
          }

        //duplicate name check, the name is used as an id of sorts
        deployMethodsArray.forEach(element => {
            if ($(this).find(".deployMethodName").val() === element.name){
                duplicateName = true
                return
            }
        })

        //error Handling
        if(duplicateName){
            errorFillingOut = true;
            $("#settingsModalErrorMessage").html('Please ensure there are no duplicate deploy method names')
        }
        if(emptyName){
            errorFillingOut = true;
            $("#settingsModalErrorMessage").html('Please ensure deploy methods are named')
        }
        if(badUrl){
            errorFillingOut = true;
            $("#settingsModalErrorMessage").html('Please ensure deploy methods URLs are valid')
        }

        if(!errorFillingOut){
            deployMethod = 
            {
                name : $(this).find(".deployMethodName").val(),
                url : $(this).find(".deployMethodUrl").val(),
                deployMethod : $(this).find(".deployMethodMethod option:selected").val(),
                active : $(this).find(".deployMethodActive").val()
            }
            deployMethodsArray.push(deployMethod)
        } 
    })

    //if errors in previous step abort saving
    if(errorFillingOut){
        return;
    }else{
        $("#settingsModalErrorMessage").html('')
    }
    //save to local storage
    localStorage.setItem('deployMethods', JSON.stringify(deployMethodsArray))

    $('#settingsModal').modal('hide');

    //update the deploy options dropdown menu
    updateDeployOptionsDropdown()
}

function fillOutSettings() {
    //get setting values from local storage
    var manager_url = window.localStorage.getItem("manager_url");
    $("#settingsModalErrorMessage").html('')


    //fill settings with saved or default values
    if (!manager_url) {
        $("#managerUrlInput").val("http://localhost:8001");
    } else {
        $("#managerUrlInput").val(manager_url);
    }

    //setting up initial default deploy method
    if(!localStorage.getItem("deployMethods")){
        var deployMethodsArray = [
            {
                name : "default deployment",
                url : "http://localhost:8001/",
                deployMethod : "direct",
                active : true
            }
        ]
        localStorage.setItem('deployMethods', JSON.stringify(deployMethodsArray))
    }else{
        //get deploy methods from local storage 
        var deployMethodsArray = JSON.parse(localStorage.getItem("deployMethods"))
    }

    //fill out settings list rom deploy methods array
    var deployMethodManagerDiv = $("#DeployMethodManager")
    deployMethodManagerDiv.empty()
    console.log("filling out settings, GET Errors and Cors warning from Url check")
    deployMethodsArray.forEach(async element => {

        var urlReachable = await checkUrlStatus(element.url)
        var ReachableIcon = ""
        
        if(urlReachable){
            ReachableIcon = `<div class="settingsInputTooltip tooltip tooltipBottom urlStatusIcon urlStatusReachable" data-text="Destination URL Is Reachable, click to re-check" onclick="manualCheckUrlStatus('icon')"><i class="material-icons md-24">done</i></div>`
        }else{
            ReachableIcon = `<div class="settingsInputTooltip tooltip tooltipBottom urlStatusIcon urlStatusNotReachable" data-text="Destination URL Is Not Reachable, click to re-check" onclick="manualCheckUrlStatus('icon')"><i class="material-icons md-24">close</i></div>`
        }

        var directOption =  '<option value="direct">Direct</option>'
        var helmOption =  '<option value="helm">Helm</option>'
        var restOption =  '<option value="rest">Rest</option>'

        if(element.deployMethod === "direct"){
            directOption =  '<option value="direct" selected="true">Direct</option>'
        }else if(element.deployMethod === "helm"){
            helmOption =  '<option value="helm" selected="true">Helm</option>'
        }else if(element.deployMethod === "rest"){
            restOption = '<option value="rest" selected="true">Rest</option>'
        }

        var deplpoyMethodRow = '<div class="input-group">'+
        '<div class="settingsInputTooltip tooltip tooltipBottom form-control" data-text="Deploy Option Name, This must be unique"><input type="text" placeholder="Deployment Name" class="deployMethodName" value="'+element.name+'"></div>'+
        `<div class="settingsInputTooltip tooltip tooltipBottom form-control urlInputField" data-text="Deploy Option Destination URL"><input type="text" onfocusout="manualCheckUrlStatus('focusOut')" placeholder="Destination Url" class="deployMethodUrl" value="`+element.url+`"></div>`+
        ReachableIcon+
        '<div class="settingsInputTooltip tooltip tooltipBottom form-control" data-text="Deploy Method"><select class="deployMethodMethod">'+
            directOption+
            helmOption+
            restOption+
        '</select></div>'+
        '<input type="text" class="form-control deployMethodActive" value="'+element.active+'">'+
        '<button class="btn btn-secondary btn-sm tooltip tooltipBottom" data-text="Delete Deploy Option" type="button" onclick="removeDeployMethod(event)"><i class="material-icons md-24">delete</i></button>'+
        '</div>'
        deployMethodManagerDiv.append(deplpoyMethodRow)
    });
}

function addDeployMethod(){
    var deployMethodManagerDiv = $("#DeployMethodManager")

    var directOption =  '<option value="direct" selected="true">Direct</option>'
    var helmOption =  '<option value="helm">Helm</option>'
    var restOption =  '<option value="rest">Rest</option>'

    var deplpoyMethodRow = '<div class="input-group">'+
    '<div class="settingsInputTooltip tooltip tooltipBottom form-control" data-text="Deploy Option Name, This must be unique"><input type="text" placeholder="Deployment Name" class=" deployMethodName" value=""></div>'+
    `<div class="settingsInputTooltip tooltip tooltipBottom form-control" data-text="Deploy Option Destination URL"><input type="text" onfocusout="manualCheckUrlStatus('focusOut')" placeholder="Destination Url" class="deployMethodUrl"value=""></div>`+
    `<div class="settingsInputTooltip tooltip tooltipBottom urlStatusIcon urlStatusUnknown" data-text="Destination URL status Unknown, click to check" onclick="manualCheckUrlStatus('icon')"><a class="urlStatusUnknownIcon">?</a></div>`+
    '<div class="settingsInputTooltip tooltip tooltipBottom form-control" data-text="Deploy Method"><select class="deployMethodMethod" name="Deploy Method">'+
        directOption+
        helmOption+
        restOption+
    '</select></div>'+
    '<input type="text" class="form-control deployMethodActive" value="false">'+
    '<button class="btn btn-secondary btn-sm tooltip tooltipBottom" data-text="Delete Deploy Option" type="button" onclick="removeDeployMethod(event)"><i class="material-icons md-24">delete</i></button>'+
    '</div>'
    deployMethodManagerDiv.append(deplpoyMethodRow)
}

function removeDeployMethod (e){
    $(e.target).parent().remove()
}

function makeJSON() {
    console.log("makeJSON()");

    $.ajax({
        url: "/pgt_jsonbody?pgt_name=" + pgtName,
        type: 'get',
        error: function (XMLHttpRequest, textStatus, errorThrown) {
            if (404 == XMLHttpRequest.status) {
                console.error('Server cannot locate physical graph file ' + pgtName);
            } else {
                console.error('status:' + XMLHttpRequest.status + ', status text: ' + XMLHttpRequest.statusText);
            }
        },
        success: function (data) {
            downloadText(pgtName, data);
        }
    });
}

function makePNG() {

    html2canvas(document.querySelector("#main")).then(canvas => {
        var dataURL = canvas.toDataURL("image/png");
        var data = atob(dataURL.substring("data:image/png;base64,".length)),
            asArray = new Uint8Array(data.length);

        for (var i = 0, len = data.length; i < len; ++i) {
            asArray[i] = data.charCodeAt(i);
        }

        var blob = new Blob([asArray.buffer], {type: "image/png"});
        saveAs(blob, pgtName + "_Template.png");
    });
}

function createZipFilename(graph_name) {
    return graph_name.substr(0, graph_name.lastIndexOf('.graph')) + '.zip';
}

function downloadText(filename, text) {
    const element = document.createElement('a');
    element.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(text));
    element.setAttribute('download', filename);

    element.style.display = 'none';
    document.body.appendChild(element);

    element.click();

    document.body.removeChild(element);
}

// build an object URL from the blob and 'download' it
function downloadBlob(filename, blob) {
    const url = window.URL.createObjectURL(blob);

    const element = document.createElement('a');
    element.setAttribute('href', url);
    element.setAttribute('download', filename);

    element.style.display = 'none';
    document.body.appendChild(element);

    element.click();

    window.URL.revokeObjectURL(url);
    document.body.removeChild(element);
}

function zoomToFit() {
    myDiagram.zoomToFit()
}

function handleFetchErrors(response) {
    if (!response.ok) {
        throw Error(response.statusText);
    }
    return response;
}

async function helmDeploy() {
    // Here as a placeholder until a single rest-deployment is worked out
    // This code will largely be a copy form restDeploy, but slightly different
    murl = window.localStorage.getItem("manager_url");
    if (!murl) {
        saveSettings();
        $('#settingsModal').modal('show');
        $('#settingsModal').on('hidden.bs.modal', function () {
            fillOutSettings()
            murl = window.localStorage.getItem("manager_url");
        })
    }
    var manager_url = new URL(murl);
    console.log("In Helm Deploy")

    const manager_host = manager_url.hostname;
    const manager_port = manager_url.port;
    var manager_prefix = manager_url.pathname;
    var request_mode = "cors";
    const pgt_id = $("#pg_form input[name='pgt_id']").val();
    manager_url = manager_url.toString();
    if (manager_url.endsWith('/')) {
        manager_url = manager_url.substring(0, manager_url.length - 1);
    }
    if (manager_prefix.endsWith('/')) {
        manager_prefix = manager_prefix.substring(0, manager_prefix.length - 1);
    }
    console.log("Manager URL:'" + manager_url + "'");
    console.log("Manager host:'" + manager_host + "'");
    console.log("Manager port:'" + manager_port + "'");
    console.log("Manager prefix:'" + manager_prefix + "'");
    console.log("Request mode:'" + request_mode + "'");

    // sessionId must be unique or the request will fail
    const lgName = pgtName.substring(0, pgtName.lastIndexOf("_pgt.graph"));
    const sessionId = lgName + "-" + Date.now();
    console.log("sessionId:'" + sessionId + "'");

    // build urls
    // the manager_url in this case has to point to daliuge_ood
    const create_helm_url = manager_url + "/api/helm/start";
    const pgt_url = "/gen_pg?tpl_nodes_len=1&pgt_id=" + pgtName; // TODO: tpl_nodes_len >= nodes in LG
    const node_list_url = manager_url + "/api/nodes";
    const pg_spec_url = "/gen_pg_spec";
    const create_session_url = manager_url + "/api/sessions";
    const append_graph_url = manager_url + "/api/sessions/" + sessionId + "/graph/append";
    const deploy_graph_url = manager_url + "/api/sessions/" + sessionId + "/deploy";
    const mgr_url = manager_url + "/session?sessionId=" + sessionId;
    // fetch the PGT from this server
    console.log("sending request to ", pgt_url);
    console.log("graph name:", pgtName);
    const pgt = await fetch(pgt_url, {
        method: 'GET',
    })
        .then(handleFetchErrors)
        .then(response => response.json())
        .catch(function (error) {
            showMessageModal("Error", error + "\nGetting PGT unsuccessful: Unable to continue!");
        });
    // fetch the nodelist from engine
    console.log("sending request to ", node_list_url);
    const node_list = await fetch(node_list_url, {
        method: 'GET',
        // mode: request_mode,
        // credentials: 'include',
        headers: {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'http://localhost:8084'
        },
    })
        .then(handleFetchErrors)
        .then(response => response.json())
        .catch(function (error) {
            showMessageModal('Error', error + "\nGetting node_list unsuccessful: Unable to continue!");
        });
    console.log("node_list", node_list);
    // build object containing manager data
    const pg_spec_request_data = {
        manager_host: manager_host,
        node_list: node_list,
        pgt_id: pgt_id
    }

    console.log(pg_spec_request_data);
    // request pg_spec from translator
    const pg_spec_response = await fetch(pg_spec_url, {
        method: 'POST',
        mode: request_mode,
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(pg_spec_request_data)
    })
        .then(handleFetchErrors)
        .then(response => response.json())
        .catch(function (error) {
            showMessageModal('Error', error + "\nGetting pg_spec unsuccessful: Unable to continue!");
        });

    console.log("pg_spec response", pg_spec_response);
    // create session on engine
    const session_data = {"sessionId": sessionId};
    const create_session = await fetch(create_session_url, {
        credentials: 'include',
        cache: 'no-cache',
        method: 'POST',
        mode: request_mode,
        referrerPolicy: 'no-referrer',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(session_data)
    })
        .then(handleFetchErrors)
        .then(response => response.json())
        .catch(function (error) {
            showMessageModal('Error', error + "\nCreating session unsuccessful: Unable to continue!");
        });
    console.log("create session response", create_session);
    // gzip the pg_spec
    console.log(pg_spec_response.pg_spec);
    const buf = fflate.strToU8(JSON.stringify(pg_spec_response.pg_spec));
    const compressed_pg_spec = fflate.zlibSync(buf);
    console.log("compressed_pg_spec", compressed_pg_spec);

    // append graph to session on engine
    const append_graph = await fetch(append_graph_url, {
        credentials: 'include',
        method: 'POST',
        mode: request_mode,
        headers: {
            'Content-Type': 'application/json',
            'Content-Encoding': 'gzip'
        },
        referrerPolicy: 'origin',
        //body: JSON.stringify(pg_spec_response.pg_spec)
        body: new Blob([compressed_pg_spec], {type: 'application/json'})
        // body: new Blob([buf])
    })
        .then(handleFetchErrors)
        .then(response => response.json())
        .catch(function (error) {
            showMessageModal('Error', error + "\nUnable to continue!");
        });
    console.log("append graph response", append_graph);
    // deploy graph
    // NOTE: URLSearchParams here turns the object into a x-www-form-urlencoded form
    const deploy_graph = await fetch(deploy_graph_url, {
        credentials: 'include',
        method: 'POST',
        mode: request_mode,
        body: new URLSearchParams({
            'completed': pg_spec_response.root_uids,
        })
    })
        .then(handleFetchErrors)
        .then(response => response.json())
        .catch(function (error) {
            showMessageModal('Error', error + "\nUnable to continue!");
        });
    //showMessageModal("Chart deployed" , "Check the dashboard of your k8s cluster for status updates.");
    console.log("deploy graph response", deploy_graph);
    // Open DIM session page in new tab
    // Until we have somewhere else to re-direct helm deployments. This is probably for the best.
    //window.open(mgr_url, '_blank').focus();
}


async function restDeploy() {
    // fetch manager host and port from local storage
    murl = window.localStorage.getItem("manager_url");
    if (!murl) {
        saveSettings();
        $('#settingsModal').modal('show');
        $('#settingsModal').on('hidden.bs.modal', function () {
            fillOutSettings()
            murl = window.localStorage.getItem("manager_url");
        })
    };
    var manager_url = new URL(murl);
    console.log("In REST Deploy")

    const manager_host = manager_url.hostname;
    const manager_port = manager_url.port;
    var manager_prefix = manager_url.pathname;
    var request_mode = "cors";
    const pgt_id = $("#pg_form input[name='pgt_id']").val();
    manager_url = manager_url.toString();
    if (manager_url.endsWith('/')) {
        manager_url = manager_url.substring(0, manager_url.length - 1);
    }
    if (manager_prefix.endsWith('/')) {
        manager_prefix = manager_prefix.substring(0, manager_prefix.length - 1);
    }
    console.log("Manager URL:'" + manager_url + "'");
    console.log("Manager host:'" + manager_host + "'");
    console.log("Manager port:'" + manager_port + "'");
    console.log("Manager prefix:'" + manager_prefix + "'");
    console.log("Request mode:'" + request_mode + "'");

    // sessionId must be unique or the request will fail
    const lgName = pgtName.substring(0, pgtName.lastIndexOf("_pgt.graph"));
    const sessionId = lgName + "-" + Date.now();
    console.log("sessionId:'" + sessionId + "'");

    // build urls
    // the manager_url in this case has to point to daliuge_ood
    const create_slurm_url = manager_url + "/api/slurm/script";
    const pgt_url = "/gen_pg?tpl_nodes_len=1&pgt_id=" + pgtName; // TODO: tpl_nodes_len >= nodes in LG
    // const pg_spec_url        = "/gen_pg_spec";
    // const node_list_url      = manager_url + "/api/nodes";
    // const create_session_url = manager_url + "/api/sessions";
    // const append_graph_url   = manager_url + "/api/sessions/" + sessionId + "/graph/append";
    // const deploy_graph_url   = manager_url + "/api/sessions/" + sessionId + "/deploy";
    // const mgr_url            = manager_url + "/session?sessionId=" + sessionId;

    // fetch the PGT from this server
    console.log("sending request to ", pgt_url);
    console.log("graph name:", pgtName);
    const pgt = await fetch(pgt_url, {
        method: 'GET',
    })
        .then(handleFetchErrors)
        .then(response => response.json())
        .catch(function (error) {
            showMessageModal('Error', error + "\nGetting PGT unsuccessful: Unable to continue!");
        });

    // This is for a deferred start of daliuge, e.g. on SLURM
    console.log("sending request to ", create_slurm_url);
    var body = [pgtName, pgt]; // we send the name in the body with the pgt
    // console.log("Sending PGT with name:", body);
    const slurm_script = await fetch(create_slurm_url, {
        method: 'POST',
        credentials: 'include',
        cache: 'no-cache',
        mode: request_mode,
        referrerPolicy: 'no-referrer',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
        redirect: 'follow'
    })
        .then(handleFetchErrors)
        .then(response => {
            if (response.redirected) {
                // window.location.href = response.url;
                window.open(response.url, 'deploy_target').focus();
            }
        })
        .catch(function (error) {
            showMessageModal('Error', error + "\nSending PGT to backend unsuccessful!");
        });



// All the rest here is when the managers are actually running
// TODO: need to enable this again.

//   // fetch the nodelist from engine
//   console.log("sending request to ", node_list_url);
//   const node_list = await fetch(node_list_url, {
//     method: 'GET',
//     // mode: request_mode,
//     // credentials: 'include',
//     headers: {
//       'Content-Type': 'application/x-www-form-urlencoded',
//       'Origin': 'http://localhost:8084'
//     },
//   })
//   .then(handleFetchErrors)
//   .then(response => response.json())
//   .catch(function(error){
//     showMessageModal('Error', error + "\nGetting node_list unsuccessful: Unable to continue!");
//   });

//   console.log("node_list", node_list);
//   // build object containing manager data
//   const pg_spec_request_data = {
//       manager_host: manager_host,
//       node_list: node_list,
//       pgt_id: pgt_id
//   }

//   console.log(pg_spec_request_data);
//   // request pg_spec from translator
//   const pg_spec_response = await fetch(pg_spec_url, {
//     method: 'POST',
//     mode: request_mode,
//     headers: {
//       'Content-Type': 'application/json',
//     },
//     body: JSON.stringify(pg_spec_request_data)
//   })
//   .then(handleFetchErrors)
//   .then(response => response.json())
//   .catch(function(error){
//     showMessageModal('Error', error + "\nGetting pg_spec unsuccessful: Unable to continue!");
//   });

//   console.log("pg_spec response", pg_spec_response);

//   // create session on engine
//   const session_data = {"sessionId": sessionId};
//   const create_session = await fetch(create_session_url, {
//     credentials: 'include',
//     cache: 'no-cache',
//     method: 'POST',
//     mode: request_mode,
//     referrerPolicy: 'no-referrer',
//     headers: {
//       'Content-Type': 'application/json',
//     },
//     body: JSON.stringify(session_data)
//   })
//   .then(handleFetchErrors)
//   .then(response => response.json())
//   .catch(function(error){
//     showMessageModal('Error', error + "\nCreating session unsuccessful: Unable to continue!");
//   });

//   console.log("create session response", create_session);

//   // gzip the pg_spec
//   const buf = fflate.strToU8(JSON.stringify(pg_spec_response.pg_spec));
//   const compressed_pg_spec = fflate.zlibSync(buf);
//   console.log("compressed_pg_spec", compressed_pg_spec);

//   // append graph to session on engine
//   const append_graph = await fetch(append_graph_url, {
//     credentials: 'include',
//     method: 'POST',
//     mode: request_mode,
//     headers: {
//       'Content-Type': 'application/json',
//       'Content-Encoding': 'gzip'
//     },
//     body: new Blob([compressed_pg_spec])
//     // body: new Blob([buf])
//   })
//   .then(handleFetchErrors)
//   .then(response => response.json())
//   .catch(function(error){
//     showMessageModal('Error', error + "\nUnable to continue!");
//   });
//   console.log("append graph response", append_graph);

//   // deploy graph
//   // NOTE: URLSearchParams here turns the object into a x-www-form-urlencoded form
//   const deploy_graph = await fetch(deploy_graph_url, {
//     credentials: 'include',
//     method: 'POST',
//     mode: request_mode,
//     body: new URLSearchParams({
//       'completed': pg_spec_response.root_uids,
//     })
//   })
//   .then(handleFetchErrors)
//   .then(response => response.json())
//   .catch(function(error){
//     showMessageModal('Error', error + "\nUnable to continue!");
//   });

//   console.log("deploy graph response", deploy_graph);
//   // Open DIM session page in new tab
//   window.open(mgr_url, '_blank').focus();
}

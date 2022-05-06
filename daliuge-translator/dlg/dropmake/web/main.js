$(document).ready(function () {
    // jquery starts here
    //hides the dropdown navbar elements when stopping hovering over the element
    $(".dropdown-menu").mouseleave(function () {
        $(".dropdown-menu").dropdown('hide')
    })

    //handles switching of the dynamic deploy split button
    $("#deployDropdowns .dropdown-menu .dropdown-item").click(function(){
        //take note of previous main button and the one that was just pressed

        var oldActive = $("#deployDropdowns").children()[0];
        var oldActiveId = $(oldActive).attr("id")
        var newActive = event.target
        var newActiveId = $(newActive).attr("id")

        //replaces main button
        $("#deployDropdowns").children()[0].remove()
        $(newActive).clone().prependTo($("#deployDropdowns"))
        
        //toggles dropdown options
        $("#deployDropdowns .dropdown-menu #"+newActiveId).hide()
        $("#deployDropdowns .dropdown-menu #"+oldActiveId).show()
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

    //keyboard shortcuts
    $(document).keydown(function(e){
        if (e.which == 79) //open settings modal on o
        {
            $('#settingsModal').modal('toggle')
        };
    })
});

function deployAction(){
    $("#gen_pg_button").val("Generate &amp; Deploy Physical Graph")
    $("#dlg_mgr_deploy").prop("checked", true)
    $("#pg_form").submit();
}

function helmDeployAction(){
    $("#gen_helm_button").val("Generate &amp; Deploy Physical Graph")
    $("#dlg_helm_deploy").prop("checked", true)
    $("#pg_helm_form").submit()
}

function restDeployAction(){
    restDeploy()
}

function saveSettings() {
    //need a check function to confirm settings have been filled out correctly

    var newUrl = new URL($("#managerUrlInput").val());
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
    $('#settingsModal').modal('hide');

    var settingsDeployMethods = $("#DeployMethodManager .input-group")//deploy method rows selector
    var deployMethodsArray = []//temp array of deploy method rows values
    
    settingsDeployMethods.each(function(){
        deployMethod = 
            {
                name : $(this).find(".deployMethodName").val(),
                url : $(this).find(".deployMethodUrl").val(),
                deployMethod : $(this).find(".deployMethodMethod option:selected").val()
            }
        deployMethodsArray.push(deployMethod)
    })
    localStorage.setItem('deployMethods', JSON.stringify(deployMethodsArray))
}

function fillOutSettings() {
    //get setting values from local storage
    var manager_url = window.localStorage.getItem("manager_url");

    //fill settings with saved or default values
    if (!manager_url) {
        $("#managerUrlInput").val("http://localhost:8001");
    } else {
        $("#managerUrlInput").val(manager_url);
    }

    //setting up initial default deploy method
    if(!localStorage.getItem("deployMethods")){
        console.log("setting up default")
        var deployMethodsArray = [
            {
                name : "default deployment",
                url : "http://localhost:8001/",
                deployMethod : "direct"
            }
        ]
        localStorage.setItem('deployMethods', JSON.stringify(deployMethodsArray))
    }else{
        console.log("getting array")

        var deployMethodsArray = JSON.parse(localStorage.getItem("deployMethods"))
    }

    var deployMethodManagerDiv = $("#DeployMethodManager")
    deployMethodManagerDiv.empty()
    deployMethodsArray.forEach(element => {

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
        '<input type="text" placeholder="Deployment Name" class="form-control deployMethodName" value="'+element.name+'">'+
        '<input type="text" placeholder="Destination Url" class="form-control deployMethodUrl" value="'+element.url+'">'+
        '<select class="form-control deployMethodMethod" name="Deploy Method">'+
            directOption+
            helmOption+
            restOption+
        '</select>'+
        '<button class="btn btn-secondary btn-sm" type="button" onclick="removeDeployMethod(event)"><i class="material-icons md-24">delete</i></button>'+
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
    '<input type="text" placeholder="Deployment Name" class="form-control deployMethodName" value="">'+
    '<input type="text" placeholder="Destination Url" class="form-control deployMethodUrl" value="">'+
    '<select class="form-control deployMethodMethod" name="Deploy Method">'+
        directOption+
        helmOption+
        restOption+
    '</select>'+
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
            alert(error + "\nGetting PGT unsuccessful: Unable to contiune!");
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
            alert(error + "\nSending PGT to backend unsuccessfull!");
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
//     alert(error + "\nGetting node_list unsuccessful: Unable to contiune!");
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
//     alert(error + "\nGetting pg_spec unsuccessful: Unable to contiune!");
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
//     alert(error + "\nCreating session unsuccessful: Unable to contiune!");
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
//     alert(error + "\nUnable to contiune!");
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
//     alert(error + "\nUnable to contiune!");
//   });

//   console.log("deploy graph response", deploy_graph);
//   // Open DIM session page in new tab
//   window.open(mgr_url, '_blank').focus();
}

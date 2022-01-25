$( document ).ready(function() {
  // jquery starts here

  //hides the dropdown navbar elements when stopping hovering over the element
  $(".dropdown-menu").mouseleave(function(){
    $(".dropdown-menu").dropdown('hide')
  })

  $('#rest_deploy_button').click(restDeploy);

  //deploy physical graph button listener
  $("#deploy_button").click(function(){
    $("#gen_pg_button").val("Generate &amp; Deploy Physical Graph")
    $("#dlg_mgr_deploy").prop( "checked", true )
    $("#pg_form").submit();
  })

  //export physical graph button listener
  $("#Pysical_graph").click(function(){
    $("#gen_pg_button").val("Generate Physical Graph")
    $("#dlg_mgr_deploy").prop( "checked", false )
    $("#pg_form").submit();
  })

  //get saved settings from local storage or set a default value
  fillOutSettings()

  $('#settingsModal').on('hidden.bs.modal', function () {
    fillOutSettings()
});
});

function saveSettings(){
  var newUrl = new URL($("#managerUrlInput").val());
  var newPort = newUrl.port;
  var newHost = newUrl.hostname;
  var newPrefix = newUrl.pathname;
  var newProtocol = newUrl.protocol;
  console.log("URL set to:'"+newUrl+"'");
  console.log("Protocol set to:'"+newProtocol+"'");
  console.log("Host set to:'"+newHost+"'");
  console.log("Port set to:'"+newPort+"'");
  console.log("Prefix set to:'"+newPrefix+"'");

  window.localStorage.setItem("manager_url", newUrl);
  window.localStorage.setItem("manager_protocol", newProtocol);
  window.localStorage.setItem("manager_host", newHost);
  window.localStorage.setItem("manager_port", newPort);
  window.localStorage.setItem("manager_prefix", newPrefix);
  $('#settingsModal').modal('hide')
}

function fillOutSettings(){
  //get setting values from local storage
  var manager_url = window.localStorage.getItem("manager_url");

  //fill settings with saved or default values
  if (!manager_url){
    $("#managerUrlInput").val("http://localhost:8001");
  }else{
    $("#managerUrlInput").val(manager_url);
  };
}

function makeJSON() {
    console.log("makeJSON()");

    $.ajax({
        url: "/pgt_jsonbody?pgt_name="+pgtName,
        type: 'get',
        error: function(XMLHttpRequest, textStatus, errorThrown) {
          if (404 == XMLHttpRequest.status) {
            console.error('Server cannot locate physical graph file '+pgtName);
          } else {
            console.error('status:' + XMLHttpRequest.status + ', status text: ' + XMLHttpRequest.statusText);
          }
        },
        success: function(data){
          downloadText(pgtName, data);
        }
    });
}

function makePNG() {

  html2canvas(document.querySelector("#main")).then(canvas => {
    var dataURL = canvas.toDataURL( "image/png" );
    var data = atob( dataURL.substring( "data:image/png;base64,".length ) ),
        asArray = new Uint8Array(data.length);

    for( var i = 0, len = data.length; i < len; ++i ) {
        asArray[i] = data.charCodeAt(i);
    }

    var blob = new Blob( [ asArray.buffer ], {type: "image/png"} );
    saveAs(blob, pgtName+"_Template.png");
  });
}

function createZipFilename(graph_name){
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


async function restDeploy(){
  // fetch manager host and port from local storage
  murl = window.localStorage.getItem("manager_url");
  if (!murl){
    saveSettings();
    $('#settingsModal').modal('show');
    $('#settingsModal').on('hidden.bs.modal', function () {
      fillOutSettings()
      murl = window.localStorage.getItem("manager_url");
    })};
  var manager_url = new URL(murl);
  console.log("In REST Deploy")
  
  const manager_host   = manager_url.hostname;
  const manager_port   = manager_url.port;
  var manager_prefix = manager_url.pathname;
  var request_mode = "cors";
  const pgt_id         = $("#pg_form input[name='pgt_id']").val();
  manager_url = manager_url.toString();
  if (manager_url.endsWith('/')){
    manager_url = manager_url.substring(0, manager_url.length - 1);
  }
  if (manager_prefix.endsWith('/')){
    manager_prefix = manager_prefix.substring(0, manager_prefix.length -1);
  }
  console.log("Manager URL:'"+manager_url+"'");
  console.log("Manager host:'"+manager_host+"'");
  console.log("Manager port:'"+manager_port+"'");
  console.log("Manager prefix:'"+manager_prefix+"'");
  console.log("Request mode:'"+request_mode+"'");

  // sessionId must be unique or the request will fail
  const lgName = pgtName.substring(0, pgtName.lastIndexOf("_pgt.graph"));
  const sessionId = lgName + "-" + Date.now();
  console.log("sessionId:'"+sessionId+"'");

  // build urls
  // the manager_url in this case has to point to daliuge_ood
  const create_slurm_url   = manager_url + "/api/slurm/script";  
  const pgt_url            = "/gen_pg?tpl_nodes_len=1&pgt_id=" + pgtName; // TODO: tpl_nodes_len >= nodes in LG
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
  .catch(function(error){
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
      window.location.href = response.url;
  }})
  .catch(function(error){
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

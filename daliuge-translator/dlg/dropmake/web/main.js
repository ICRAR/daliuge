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

  //erport physical graph button listener
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
  var newPort = $("#managerPortInput").val();
  var newHost = $("#managerHostInput").val().replace(/\s+/g, '');
  var newPrefix = $("#managerPrefixInput").val().replace(/\s+/g, '');
  console.log("Host set to:'"+newHost+"'");
  console.log("Port set to:'"+newPort+"'");
  console.log("Prefix set to:'"+newPrefix+"'");

  window.localStorage.setItem("manager_port", newPort);
  window.localStorage.setItem("manager_host", newHost);
  window.localStorage.setItem("manager_prefix", newPrefix);
  $('#settingsModal').modal('hide')
}

function fillOutSettings(){
  //get setting values from local storage
  var manager_host = window.localStorage.getItem("manager_host");
  var manager_port = window.localStorage.getItem("manager_port");
  var manager_prefix = window.localStorage.getItem("manager_prefix");

  //fill settings with saved or default values
  if (!manager_host){
    $("#managerHostInput").val("localhost");
  }else{
    $("#managerHostInput").val(manager_host);
  };

  if (!manager_port){
    $("#managerPortInput").val("8001");
  }else{
    $("#managerPortInput").val(manager_port);
  };
  if (!manager_prefix){
    $("#managerPrefixInput").val("");
  }else{
    $("#managerPrefixInput").val(manager_prefix);
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

  async function restDeploy(){
    // fetch manager host and port from HTML
    const manager_host = $('#managerHostInput').val();
    const manager_port = $('#managerPortInput').val();

    const node_list_url      = "http://" + manager_host + ":" + manager_port + "/api/nodes";
    const create_session_url = "http://" + manager_host + ":" + manager_port + "/api/sessions";
    const append_graph_url   = "http://" + manager_host + ":" + manager_port + "/api/sessions/testSession/graph/append";
    const deploy_graph_url   = "http://" + manager_host + ":" + manager_port + "/api/sessions/testSession/deploy";

    // sessionId must be unique or the request will fail
    const sessionId = pgtName + Date.now();

    // fetch the nodelist
    const node_list = await fetch(node_list_url).then(response => response.json());
    console.log("node_list", node_list);

    // create session
    const session_data = {sessionId: sessionId};
    const create_session = await fetch(create_session_url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(session_data)
    }).then(response => response.json());
    console.log("create session response", create_session);

    // TODO: gzip the graph
    gzipped_graph = null;

    // append graph
    const append_graph = await fetch(append_graph_url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Encoding': 'gzip'
      },
      body: gzipped_graph
    }).then(response => response.json());
    console.log("append graph response", append_graph);
  }

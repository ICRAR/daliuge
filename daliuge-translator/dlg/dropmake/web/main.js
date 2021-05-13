$( document ).ready(function() {
  // jquery starts here

  //hides the dropdown navbar elements when stopping hovering over the element
  $(".dropdown-menu").mouseleave(function(){
    $(".dropdown-menu").dropdown('hide')
  })

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
  var newHost = $("#managerHostInput").val();

  window.localStorage.setItem("manager_port", newPort);
  window.localStorage.setItem("manager_host", newHost);
  $('#settingsModal').modal('hide')    
}

function fillOutSettings(){
  //get setting values from local storage
  var manager_host = window.localStorage.getItem("manager_host");
  var manager_port = window.localStorage.getItem("manager_port");

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
}

var lastStroked = null;  // this remembers the last highlit Shape
  function init() {
    var $ = go.GraphObject.make;  // for conciseness in defining templates

    myDiagram =
      $(go.Diagram, "myDiagram",  // must be the ID or reference to div
        {
          initialAutoScale: go.Diagram.UniformToFill,
          layout: $(go.LayeredDigraphLayout),
          // other Layout properties are set by the layout function, defined below
          mouseOver: doMouseOver  // this event handler is defined below
        });

      myDiagram.nodeTemplateMap.add("",
        $(go.Node, "Spot",
          { locationSpot: go.Spot.Center },
          $(go.Shape, "Circle",
            { fill: "lightgray",  // the initial value, but data-binding may provide different value
              stroke: "black",
              desiredSize: new go.Size(40, 40) },
            new go.Binding("fill", "fill")),
          $(go.TextBlock,
            new go.Binding("text", "text"))
        ));

        myDiagram.nodeTemplateMap.add("Component",
          $(go.Node, "Spot",
            { locationSpot: go.Spot.Center },
            $(go.Shape, "Rectangle",
              { name: "SHAPE",
                fill: "#00A9C9",  // the initial value, but data-binding may provide different value
                stroke: "#ffffff",
                desiredSize: new go.Size(100, 30) },
              new go.Binding("fill", "fill")),
            $(go.TextBlock,
              {
                  stroke: "whitesmoke",
                  textAlign: "center"
              },
              new go.Binding("text", "text"))
          ));

          myDiagram.nodeTemplateMap.add("Data",
            $(go.Node, "Spot",
              { locationSpot: go.Spot.Center },
              $(go.Shape, "Parallelogram2",
                { name: "SHAPE",
                  fill: "#004080",  // the initial value, but data-binding may provide different value
                  stroke: "#ffffff",
                  desiredSize: new go.Size(100, 30) },
                new go.Binding("fill", "fill")),
              $(go.TextBlock,
                {
                    stroke: "whitesmoke",
                    textAlign: "center"
                },
                new go.Binding("text", "text"))
            ));

            // define the group template
        myDiagram.groupTemplate =
          $(go.Group, "Auto",
            { // define the group's internal layout
              layout: $(go.TreeLayout,
                        { angle: 90, arrangement: go.TreeLayout.ArrangementHorizontal, isRealtime: false }),
              // the group begins unexpanded;
              // upon expansion, a Diagram Listener will generate contents for the group
              isSubGraphExpanded: false

            },
            $(go.Shape, "Rectangle",
              { fill: null, stroke: "gray", strokeWidth: 2 }),
            $(go.Panel, "Vertical",
              { defaultAlignment: go.Spot.Left, margin: 4 },
              $(go.Panel, "Horizontal",
                { defaultAlignment: go.Spot.Top },
                // the SubGraphExpanderButton is a panel that functions as a button to expand or collapse the subGraph
                $("SubGraphExpanderButton"),
                $(go.TextBlock,
                  { font: "Bold 18px Sans-Serif", margin: 4 },
                  new go.Binding("text", "text"))
              ),
              // create a placeholder to represent the area where the contents of the group are
              $(go.Placeholder,
                { padding: new go.Margin(0, 10) })
            )  // end Vertical Panel
          );  // end Group


    // define the Link template to be minimal
    myDiagram.linkTemplate =
      $(go.Link,
        { selectable: false },
        $(go.Shape,  // the link path shape
          { isPanelMain: true, stroke: "gray", strokeWidth: 2 }),
          $(go.Shape,  // the arrowhead
            { toArrow: "standard", stroke: null, fill: "gray"})
        );

    // generate a tree with the default values
    //rebuildGraph();
    loadFromRemoteJson();

    // Make sure the infoBox is momentarily hidden if the user tries to mouse over it
    var infoBoxH = document.getElementById("infoBoxHolder");
    infoBoxH.addEventListener("mousemove", function() {
      var box = document.getElementById("infoBoxHolder");
      box.style.left = parseInt(box.style.left) + "px";
      box.style.top = parseInt(box.style.top)+30 + "px";
    }, false);

    var diagramDiv = document.getElementById("myDiagram");

    // Make sure the infoBox is hidden when the mouse is not over the Diagram
    diagramDiv.addEventListener("mouseout", function(e) {
      if (lastStroked !== null) lastStroked.stroke = null;
      lastStroked = null;

      var infoBox = document.getElementById("infoBox");
      var elem = document.elementFromPoint(e.clientX, e.clientY);
      if (elem !== null && (elem === infoBox || elem.parentNode === infoBox)) {
        var box = document.getElementById("infoBoxHolder");
        box.style.left = parseInt(box.style.left) + "px";
        box.style.top = parseInt(box.style.top)+30 + "px";
      } else {
        var box = document.getElementById("infoBoxHolder");
        box.innerHTML = "";
      }
    }, false);

    // // add event listener to the "deploy" checkbox so that when the checkbox is used,
    // // the form submit button is updated to reflect the behaviour
    // var deployCheckbox = document.getElementById('dlg_mgr_deploy');
    // deployCheckbox.addEventListener("change", function(){
    //     if (deployCheckbox.checked){
    //         document.getElementById('gen_pg_button').value = "Generate &amp; Deploy Physical Graph";
    //     } else {
    //         document.getElementById('gen_pg_button').value = "Generate Physical Graph";
    //     }
    // });
  } // end init

  // Called when the mouse is over the diagram's background
  function doMouseOver(e) {
      if (e === undefined) e = myDiagram.lastInput;
      var doc = e.documentPoint;
      // find all Nodes that are within 100 units
      var list = myDiagram.findObjectsNear(doc, 100, null, function(x) { return x instanceof go.Node; });
      // now find the one that is closest to e.documentPoint
      var closest = null;
      var closestDist = 999999999;
      list.each(function(node) {
        var dist = doc.distanceSquaredPoint(node.getDocumentPoint(go.Spot.Center));
        if (dist < closestDist) {
          closestDist = dist;
          closest = node;
        }
      });
      highlightNode(e, closest);
    }

    // Called with a Node (or null) that the mouse is over or near
  function highlightNode(e, node) {
    if (node !== null) {
      var shape = node.findObject("SHAPE");
      if (shape !== null) {
        shape.stroke = "white";
      }
      if (lastStroked !== null && lastStroked !== shape) lastStroked.stroke = null;
      lastStroked = shape;
      updateInfoBox(e.viewPoint, node.data);
    } else {
      if (lastStroked !== null) lastStroked.stroke = null;
      lastStroked = null;
      document.getElementById("infoBoxHolder").innerHTML = "";
    }
  }

  function updateInfoBox(mousePt, data) {
    var x =
    "<div id='infoBox'>" +
    "<div>" + data.oid + "</div>" +
    "<div class='infoTitle'>Type</div>" +
    "<div class='infoValues'>" + data.category + "</div><br/>" +
    "<div class='infoTitle'>Name</div>" +
    "<div class='infoValues'>" + data.text + "</div><br/>" +
    "<div class='infoTitle'>Key</div>" +
    "<div class='infoValues'>" + data.key + "</div>" +
    "</div>"

    var box = document.getElementById("infoBoxHolder");
    box.innerHTML = x;
    box.style.left = mousePt.x+70 + "px";
    box.style.top = mousePt.y+20 + "px";
  }

  function loadFromRemoteJson() {
    //given a logical graph name, get its JSON from the server
    //alert("Previous lg name = " + window.curr_lg_name);
    //alert("Requesting " + pgtName.toString());
    $.ajax({
      url: "/pgt_jsonbody?pgt_name="+pgtName,
      type: 'get',
      error: function(XMLHttpRequest, textStatus, errorThrown) {
          if (404 == XMLHttpRequest.status) {
            alert('Server cannot locate physical graph file ' + pgtName.toString())
          } else {
            alert('status:' + XMLHttpRequest.status + ', status text: ' + XMLHttpRequest.statusText);
          }
      },
      success: function(data){
        //console.log(data);
        myDiagram.model = go.Model.fromJson(data);
      }
    });
  }

  function rebuildGraph() {
    var minNodes = document.getElementById("minNodes").value;
    minNodes = parseInt(minNodes, 10);

    var maxNodes = document.getElementById("maxNodes").value;
    maxNodes = parseInt(maxNodes, 10);

    generateDigraph(minNodes, maxNodes);
  }

  function generateDigraph(minNodes, maxNodes) {
    myDiagram.startTransaction("generateDigraph");
    // replace the diagram's model's nodeDataArray
    generateNodes(minNodes, maxNodes);
    // replace the diagram's model's linkDataArray
    generateLinks();
    // force a diagram layout
    layout();
    myDiagram.commitTransaction("generateDigraph");
  }

  // Creates a random number of randomly colored nodes.
  function generateNodes(minNodes, maxNodes) {
    var nodeArray = [];
    // get the values from the fields and create a random number of nodes within the range
    var min = parseInt(minNodes, 10);
    var max = parseInt(maxNodes, 10);
    if (isNaN(min)) min = 0;
    if (isNaN(max) || max < min) max = min;
    var numNodes = Math.floor(Math.random() * (max - min + 1)) + min;
    var i;
    for (i = 0; i < numNodes; i++) {
      var cat;
      if (i % 2 == 0) {
        cat = "Data"
      } else {
        cat = "Component"
      }
      nodeArray.push({
        key: i,
        text: i.toString(),
        //fill: go.Brush.randomColor(),
        category: cat
      });
    }

    // randomize the node data
    for (i = 0; i < nodeArray.length; i++) {
      var swap = Math.floor(Math.random() * nodeArray.length);
      var temp = nodeArray[swap];
      nodeArray[swap] = nodeArray[i];
      nodeArray[i] = temp;
    }

    // set the nodeDataArray to this array of objects
    myDiagram.model.nodeDataArray = nodeArray;
  }

  // Create some link data
  function generateLinks() {
    if (myDiagram.nodes.count < 2) return;
    var linkArray = [];
    var nit = myDiagram.nodes;
    var nodes = new go.List(go.Node);
    nodes.addAll(nit);
    for (var i = 0; i < nodes.count - 1; i++) {
      var from = nodes.elt(i);
      var numto = Math.floor(1 + (Math.random() * 3) / 2);
      for (var j = 0; j < numto; j++) {
        var idx = Math.floor(i + 5 + Math.random() * 10);
        if (idx >= nodes.count) idx = i + (Math.random() * (nodes.count - i)) | 0;
        var to = nodes.elt(idx);
        linkArray.push({ from: from.data.key, to: to.data.key });
      }
    }
    myDiagram.model.linkDataArray = linkArray;
  }

  function layout() {
    myDiagram.startTransaction("change Layout");
    var lay = myDiagram.layout;

    var direction = getRadioValue("direction");
    direction = parseFloat(direction, 10);
    lay.direction = direction;

    //var layerSpacing = document.getElementById("layerSpacing").value;
    var layerSpacing = 25; //parseFloat(layerSpacing, 10);
    lay.layerSpacing = layerSpacing;

    //var columnSpacing = document.getElementById("columnSpacing").value;
    var columnSpacing = 25; //parseFloat(columnSpacing, 10);
    lay.columnSpacing = columnSpacing;

    /*
    var cycleRemove = getRadioValue("cycleRemove");
    if (cycleRemove === "CycleDepthFirst") lay.cycleRemoveOption = go.LayeredDigraphLayout.CycleDepthFirst;
    else if (cycleRemove === "CycleGreedy") lay.cycleRemoveOption = go.LayeredDigraphLayout.CycleGreedy;
    */
    lay.cycleRemoveOption = go.LayeredDigraphLayout.CycleDepthFirst;

    /*
    var layering = getRadioValue("layering");
    if (layering === "LayerOptimalLinkLength") lay.layeringOption = go.LayeredDigraphLayout.LayerOptimalLinkLength;
    else if (layering === "LayerLongestPathSource") lay.layeringOption = go.LayeredDigraphLayout.LayerLongestPathSource;
    else if (layering === "LayerLongestPathSink") lay.layeringOption = go.LayeredDigraphLayout.LayerLongestPathSink;
    */
    lay.layeringOption = go.LayeredDigraphLayout.LayerOptimalLinkLength;

    /*
    var initialize = getRadioValue("initialize");
    if (initialize === "InitDepthFirstOut") lay.initializeOption = go.LayeredDigraphLayout.InitDepthFirstOut;
    else if (initialize === "InitDepthFirstIn") lay.initializeOption = go.LayeredDigraphLayout.InitDepthFirstIn;
    else if (initialize === "InitNaive") lay.initializeOption = go.LayeredDigraphLayout.InitNaive;
    */
    lay.initializeOption = go.LayeredDigraphLayout.InitDepthFirstIn;

    /*
    var aggressive = getRadioValue("aggressive");
    if (aggressive === "AggressiveLess") lay.aggressiveOption = go.LayeredDigraphLayout.AggressiveLess;
    else if (aggressive === "AggressiveNone") lay.aggressiveOption = go.LayeredDigraphLayout.AggressiveNone;
    else if (aggressive === "AggressiveMore") lay.aggressiveOption = go.LayeredDigraphLayout.AggressiveMore;
    */
    lay.aggressiveOption = go.LayeredDigraphLayout.AggressiveLess;

    //TODO implement pack option
    var pack = document.getElementsByName("pack");
    var packing = 0;
    for (var i = 0; i < pack.length; i++) {
      if (pack[i].checked) packing = packing | parseInt(pack[i].value, 10);
    }
    lay.packOption = packing;

    /*
    var setsPortSpots = document.getElementById("setsPortSpots");
    lay.setsPortSpots = setsPortSpots.checked;
    */
    lay.setsPortSpots = true;

    myDiagram.commitTransaction("change Layout");
  }

  function getRadioValue(name) {
    var radio = document.getElementsByName(name);
    for (var i = 0; i < radio.length; i++)
      if (radio[i].checked) return radio[i].value;
  }

  function genGanttChart() {
    url = "/show_gantt_chart?pgt_id="+pgtName
    window.open(url)
  }

  function genScheduleChart() {
    url = "/show_schedule_mat?pgt_id="+pgtName
    window.open(url)
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
    //zoomToFit();

    //var svg = myDiagram.makeSvg({
    var rect_w = myDiagram.viewportBounds.width;
    var rect_h = myDiagram.viewportBounds.height;
    var img_w = myDiagram.documentBounds.width;
    var img_h = myDiagram.documentBounds.height;

    w_ratio = rect_w / img_w;
    h_ratio = rect_h / img_h;

    var scale_f = Math.min(1.0, Math.min(w_ratio, h_ratio));

    var svg = myDiagram.makeImage({
        scale: scale_f,
        background: "White",
        details: 1.0
      });
    svg.style.border = "1px solid black";

    obj = document.getElementById("SVGArea");
    obj.appendChild(svg);
    if (obj.children.length > 0) {
      obj.replaceChild(svg, obj.children[0]);
    }
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

  function makeCWL() {
    var error = "";

    fetch('/pgt_cwl?pgt_name='+pgtName)
      .then(async resp => {
        // if fetch was not successful, await the error message in the body of the response
        if (resp.status !== 200){
            error = await resp.text();
            return;
        }
        return resp.blob();
      })
      .then(blob => {
        downloadBlob(createZipFilename(pgtName), blob);
      })
      .catch(() => alert(error)); // present error, if it occurred
  }

  function zoomToFit() {
    myDiagram.zoomToFit()
    // console.log(myDiagram.viewportBounds.width.toString());
    // console.log('\n');
    // console.log(myDiagram.viewportBounds.height.toString());
    // console.log('\n -----');
    // console.log(myDiagram.documentBounds.width.toString());
    // console.log('\n');
    // console.log(myDiagram.documentBounds.height.toString());
  }

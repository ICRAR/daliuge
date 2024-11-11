//require takes over the whole page, thus we need to load main.js with require as well
require([
    "/static/main.js",
]);

function showMessageModal(title, content) {
    $("#messageModalTitle").html(title);
    $("#messageModalContent").html(content);
    $('#messageModal').modal('show');
}

function graphInit(graphType) {

    $.ajax({
        //get data
        url: "/pgt_jsonbody?pgt_name=" + pgtName,
        dataType: "json",
        type: 'get',
        error: function (XMLHttpRequest, textStatus, errorThrown) {
            if (404 == XMLHttpRequest.status) {
                showMessageModal('Error', 'Server cannot locate physical graph file: ' + pgtName.toString());
            } else {
                showMessageModal('Error', 'status:' + XMLHttpRequest.status + ', status text: ' + XMLHttpRequest.statusText);
            }
        },
        success: function (data) {
            // get node count
            var nodeCount = 0;
            data = JSON.parse(data);
            data.nodeDataArray.forEach(element => {
                nodeCount++
            });

            console.log(data['reprodata'])

            //set initially shown graph based on node count
            if (graphType === "default") {
                if (nodeCount < 100) {
                    graphType = "dag"
                } else {
                    graphType = "sankey"
                }
            }

            //reset graph divs
            $("#main").empty()
            //initiate the correct function
            if (graphType === "sankey") {
                echartsGraphInit("sankey", data)
            } else if (graphType === "dag") {
                dagGraphInit(data)
            }

            //set correct graph button to active
            $(".graphChanger").removeClass("active")
            $("#" + graphType + "Button").addClass("active")

            //hide graph change buttons when they dont make sense locks the graph to sankey if node count is over 300
            if (nodeCount > 600) {
                $("#view-mode-buttons").hide();
            } else {
                $("#view-mode-buttons").show();
            }

            // display any errors that were generated during translation
            if (error !== "None") {
                showMessageModal("Error", error);
            }
        }
    })
};

// dag graph setup

function dagGraphInit(data) {

    const heightValue = 300;
    const widthValue = 600;

    // Set up zoom support
    d3.select("#main").append("div").attr("id", "dagGraphArea").append("svg").attr("id", "smallD3Graph").append("g").attr("id", "root")
    var svg = d3.select("#smallD3Graph")
    var inner = svg.select("g");

    //Add mouse wheel zoom event
    var zoom = d3.zoom().on("zoom", function () {
        inner.attr("transform", d3.event.transform);
    });
    svg.call(zoom);

    var g = new dagreD3.graphlib.Graph({ compound: true })
        .setGraph({
            nodesep: 70,
            ranksep: 50,
            rankdir: "LR", // Left-to-right layout
            marginx: 20,
            marginy: 20
        })
        .setDefaultEdgeLabel(function () { return {}; });

    var render = getRender();
    function drawGraph() {
        inner.call(render, g);
    }

    // initiating
    var graph_update_handler = drawGraphForDrops.bind(null, g, drawGraph);
    graph_update_handler(data)
}

function getRender() {

    var render = new dagreD3.render();

    // Add our custom shape (parallelogram, similar to the PIP PDR document)
    render.shapes().parallelogram = function (parent, bbox, node) {
        var w = bbox.width,
            h = bbox.height,
            points = [
                { x: 0, y: 0 },
                { x: w * 0.8, y: 0 },
                { x: w, y: -h },
                { x: w * 0.2, y: -h },
            ];
        var shapeSvg = parent.insert("polygon", ":first-child")
            .attr("points", points.map(function (d) { return d.x + "," + d.y; }).join(" "))
            .attr("transform", "translate(" + (-w / 2) + "," + (h / 2) + ")");

        node.intersect = function (point) {
            return dagreD3.intersect.polygon(node, points, point);
        };
        return shapeSvg;
    };
    return render;
}

function zoomFit() {

    // Center the graph
    var zoom = d3.zoom().on("zoom", function () {//Add mouse wheel zoom event
        inner.attr("transform", d3.event.transform);
    });
    var svg = d3.select('#smallD3Graph')
        ;

    var root = svg.select('#root');
    var boot = $(".output");
    var bounds = root.node().getBBox();
    var parent = root.node().parentElement;
    var fullWidth = parent.clientWidth,
        fullHeight = parent.clientHeight;
    var width = bounds.width,
        height = bounds.height,
        initialScale;
    var widthScale = ((fullWidth - 80) / width);
    var heightScale = ((fullHeight - 200) / height)
    if (heightScale < widthScale) {
        initialScale = heightScale;
    } else {
        initialScale = widthScale;
    };
    initialScale = initialScale
    var xCenterOffset = -(fullWidth - fullWidth) / 2;
    boot.attr("transform", "translate(" + (fullWidth - (width * initialScale)) / 2 + ", " + ((fullHeight - 80) - (height * initialScale)) / 2 + ")" + ' scale(' + initialScale + ')');
}

function drawGraphForDrops(g, drawGraph, data) {

    // Keep track of modifications to see if we need to re-draw
    var modified = false;

    // #1: create missing nodes in the graph
    // Because oids is sorted, they will be created in oid order
    var time0 = new Date().getTime();
    var nodes = data['nodeDataArray'];
    var links = data['linkDataArray']
    var nodes_dict = {};
    for (var idx of nodes.keys()) {
        if (idx != 'reprodata') {
            var node = nodes[idx];
        }
        if (node.oid) {
            modified |= _addNode(g, node);
            nodes_dict[node.key] = { node }
        }
    }

    var time1 = new Date().getTime();
    console.log('Took %d [ms] to create the nodes', (time1 - time0))

    // #2: establish missing relationships
    for (var idx of links.keys()) {
        var findex = links[idx]['from']
        var tindex = links[idx]['to']
        g.setEdge(nodes_dict[findex].node.key, nodes_dict[tindex].node.key, { width: 40 });
    }

    if (modified) {
        drawGraph();
    }
    zoomFit()
}

function _addNode(g, node) {
    var TYPE_SHAPES = { Component: 'rect', Data: 'parallelogram' }

    if (g.hasNode(g)) {
        return false;
    }

    var typeClass = node.category;
    var typeShape = TYPE_SHAPES[node.category];
    var notes = node.name;

    var oid = node.key;
    var html = '<div class="drop-label ' + typeShape + '" id="id_' + oid + '">';
    html += '<span class="notes">' + notes + '</span>';
    oid_date = node.oid.split("_")[0];
	human_readable_id = oid_date + "_" + node.key + "_" + node.iid
    html += '<span style="font-size: 13px;">' + human_readable_id + '</span>';
    html += "</div>";
    g.setNode(oid, {
        labelType: "html",
        label: html,
        rx: 5,
        ry: 5,
        padding: 0,
        class: typeClass,
        shape: typeShape
    });
    return true;
}



//sankey graph setup

function echartsGraphInit(type, data) {

    // echarts only displays the name, which is also the key for the edges, thus
    // we need to make sure that the labels are both meaningful and unique.
    //all nodes and edges
    var graphData = { 'nodeDataArray': [], 'linkDataArray': [] };

    //partitions
    var graphDataParts = { 'nodeDataArray': [], 'linkDataArray': [] };
    var newElement = {};
    let keyIndex = new Map();
    //shapes and colors for different node types
    var nodeCatgColors = { 'Data': '#9ab4d0', 'Component': '#7f9cbb' }
    var nodeCatgShape = { 'Data': 'path://M 300 100 L 1000 100 L 800 200 L 100 200 z', 'Component': 'rect' }
    // var nodeCount = 0
    var renderer = 'canvas'
    var fontSize = 10
    data.nodeDataArray.forEach(element => {
        newElement = {};
        if (!element.hasOwnProperty("isGroup")) {
            // helper map to fix the links later
            keyIndex.set(element.key, element.name + '-' + element.key.toString());
            //data options
            newElement.name = element.name + '-' + element.key.toString();

            newElement.label = {
                'rotate': 45,
                'fontSize': fontSize,
                'offset': [-20, -20],
                'fontWeight': 400,
                'color': element.group.toString(),
                // 'textBorderColor':'black',
                // 'textBorderWidth' : 2.5,
                // 'textBorderType' : 'solid'
            };

            newElement.itemStyle = {};
            newElement.itemStyle.color = nodeCatgColors[element.category];
            newElement.symbol = nodeCatgShape[element.category];
            newElement.symbolSize = [60, 30]
            graphData.nodeDataArray.push(newElement);
        } else {
            newElement.name = element.key.toString();
            newElement.color = 'black';
            graphDataParts.nodeDataArray.push(newElement);
        }
    });
    var numGroups = graphDataParts.nodeDataArray.length;
    var spread = 255 / Math.ceil((numGroups / 3));
    var ind = 0;
    graphDataParts.nodeDataArray.forEach(element => {
        var icol = Math.floor((256 ** (ind / (numGroups / 3)) - 1) * spread);
        element.color = "#" + icol.toString(16).padStart(6, '0');
        ind += 1;
    })
    graphData.nodeDataArray.forEach(element => {
        var group = graphDataParts.nodeDataArray.filter(
            function (item) {
                return item.name == element.label.color
            });

        // if graph was generated without partitions, then group[0] is undefined
        if (typeof group[0] !== 'undefined') {
            element.label.color = group[0].color;
        } else {
            element.label.color = 'black';
        }
    })
    data.linkDataArray.forEach(element => {
        newElement = {};
        newElement.source = keyIndex.get(element.from);
        newElement.target = keyIndex.get(element.to);
        newElement.value = 20;
        graphData.linkDataArray.push(newElement);
    });

    //append graph div
    $("#main").append("<div id='" + type + "'></div>")

    // //re-initialise new graph
    var chart = echarts.init(document.getElementById(type), null, { 'renderer': 'svg' });
    graphSetup(type, chart, graphData, graphDataParts)
}


function graphSetup(type, chart, graphData, graphDataParts) {

    // don't show labels if there are too many nodes.
    var show_labels = (graphData.nodeDataArray.length > 350) ? false : true;

    chart.setOption({
        tooltip: {
            trigger: 'item',
            triggerOn: 'mousemove'
        },
        animation: true,
        series: [{
            type: type,
            layout: 'dagre',
            symbolSize: 20,
            roam: true,
            zoom: 1.15,
            label: {
                show: show_labels
            },

            emphasis: {
                focus: 'adjacency'
            },
            nodeAlign: 'right',

            data: graphData.nodeDataArray,
            links: graphData.linkDataArray,
            lineStyle: {
                color: 'grey',
                curveness: 0.5
            }
        }]
    });
    chart.on('click', function (params) {
        console.log(params, params.series);
    });
}

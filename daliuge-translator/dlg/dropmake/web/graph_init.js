//require takes over the whole page, thus we need to load main.js with require as well
require([
    "/static/main.js",
]);

//initial initialisation of graphs
$(document).ready(function() {
    var type = "default"
    var initBtn = false
    if (pgtName.toString() !== 'None') {
        graphInit(type)
    }
});

//event listener for graph buttons
$(".graphChanger").click(function() {
    var type = $(this).val()
    $(this).addClass("active")
    var initBtn = true
    graphInit(type)
})

function graphInit(type) {
    $.ajax({
        //get data
        url: "/pgt_jsonbody?pgt_name=" + pgtName,
        dataType: "json",
        type: 'get',
        error: function(XMLHttpRequest, textStatus, errorThrown) {
            if (404 == XMLHttpRequest.status) {
                alert('Server cannot locate physical graph file ' + pgtName.toString())
            } else {
                alert('status:' + XMLHttpRequest.status + ', status text: ' + XMLHttpRequest.statusText);
            }
        },
        success: function(data) {

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
            var nodeCount = 0
            var renderer = 'canvas'
            var fontSize = 10
            data.nodeDataArray.forEach(element => {
                nodeCount++
            })
            console.log(nodeCount)
                //pick initial graph depending on node amount
            if (type === "default") {
                if (nodeCount < 100) {
                    type = "graph"
                } else {
                    type = "sankey"
                }
            }
            //hide other graph option when it doesnt make sense
            if (nodeCount < 50) {
                $(".graphChanger").hide();
                renderer = 'svg'
            } else if (nodeCount > 150) {
                $(".graphChanger").hide();
            }
            if (nodeCount > 80) {
                fontSize = 9
            }
            console.log(type)
            data.nodeDataArray.forEach(element => {
                newElement = {};
                if (!element.hasOwnProperty("isGroup")) {
                    // helper map to fix the links later
                    keyIndex.set(element.key, element.text + '-' + element.key.toString());
                    //data options
                    newElement.name = element.text + '-' + element.key.toString();

                    if (type === "sankey") {
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
                    } else {
                        newElement.label = {
                            'fontSize': fontSize,
                            'fontWeight': 400,
                            'color': element.group,
                            "position": "inside",
                            // 'textBorderColor':'black',
                            // 'textBorderWidth' : 2.5,
                        };
                    }

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
                    function(item) {
                        return item.name == element.label.color
                    });

                // if graph was generated without partitions, then group[0] is undefined
                if (typeof group[0] !== 'undefined'){
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


            //remove previous graph and active button, if any
            $("#main").empty();
            $(".graphChanger").removeClass("active")
                //add new div depending on type
            $("#main").append("<div id='" + type + "'></div>")
            $("#" + type + "Button").addClass("active")

            //re-initialise new graph
            var chart = echarts.init(document.getElementById(type), null, { 'renderer': 'svg' });
            graphSetup(type, chart, graphData, graphDataParts)
            console.log(chart.getOption())
        }
    });
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
    chart.on('click', function(params) {
        console.log(params, params.series);
    });
}
//require takes over the whole page, thus we need to load main.js with require as well
require([
    "/static/main.js",
]);

//initial initialisation of graphs
$(document).ready(function(){
    var type = "default"
    graphInit(type)
});

//event listener for graph buttons
$(".graphChanger").click(function(){
    var type = $(this).val()
    $(this).addClass("active")
    graphInit(type)
})

function graphInit(type){
    $.ajax({
        //get data
        url: "/pgt_jsonbody?pgt_name="+pgtName,
        dataType: "json",
        type: 'get',
        error: function(XMLHttpRequest, textStatus, errorThrown) {
            if (404 == XMLHttpRequest.status) {
            alert('Server cannot locate physical graph file ' + pgtName.toString())
            } else {
            alert('status:' + XMLHttpRequest.status + ', status text: ' + XMLHttpRequest.statusText);
            }
        },
        success: function(data){
 
            // echarts only displays the name, which is also the key for the edges, thus
            // we need to make sure that the labels are both meaningful and unique.
            //all nodes and edges
            var graphData = {'nodeDataArray':[], 'linkDataArray':[]};
            //partitions 
            var graphDataParts = {'nodeDataArray':[], 'linkDataArray':[]};
            var newElement = {};
            let keyIndex = new Map();
            //shapes and colors for different node types 
            var nodeCatgColors = {'Data':'#195aa0', 'Component': '#002349'}
            var nodeCatgShape = {'Data':'path://M 300 100 L 1000 100 L 800 200 L 100 200 z', 'Component':'rect'}
            data.nodeDataArray.forEach(element => {
                newElement = {};
                if (!element.hasOwnProperty("isGroup")){
                    // helper map to fix the links later
                    keyIndex.set(element.key, element.text + '-' + element.key.toString());
                    //data options
                    newElement.name = element.text + '-' + element.key.toString();
                    newElement.label = {
                        'rotate': 45,
                        'fontSize': 10,
                        'offset': [-20,-20],
                        'fontWeight' : 700,
                        'textBorderColor' : 'white',
                        'textBorderWidth' : 2,
                        'textBorderType' : 'solid'
                    };
                    newElement.itemStyle = {};
                    newElement.itemStyle.color = nodeCatgColors[element.category];
                    newElement.symbol = nodeCatgShape[element.category];
                    newElement.symbolSize = [80, 30]
                    graphData.nodeDataArray.push(newElement);
                }
                else {
                    newElement.name = element.key.toString();
                    graphDataParts.nodeDataArray.push(newElement);                  
                }
            });

            data.linkDataArray.forEach(element => {
                newElement = {};
                newElement.source = keyIndex.get(element.from);
                newElement.target = keyIndex.get(element.to);
                newElement.value = 20;
                graphData.linkDataArray.push(newElement);
            });

            //pick initial graph depending on node amount
            if(type==="default"){
                if(graphData.nodeDataArray.length<100){
                    type="graph"
                }else{
                    type="sankey"
                }
            }

            //remove previous graph and active button, if any
            $("#main").empty();
            $(".graphChanger").removeClass("active")
            //add new div depending on type
            $("#main").append("<div id='"+type+"'></div>")
            $("#"+type+"Button").addClass("active")
            //re-initialise new graph
            var chart = echarts.init(document.getElementById(type),null, {renderer:'canvas'});
            graphSetup(type, chart, graphData, graphDataParts)

        }
    });
}

function graphSetup(type, chart, graphData,graphDataParts){
  
            // don't show labels if there are too many nodes. (SETTING?)
            
            var show_labels = (graphData.nodeDataArray.length > 350) ? false:true;
         
            chart.setOption({
                layout: "dagre",
                tooltip: {
                    trigger: 'item',
                    triggerOn: 'mousemove'
                },
                animation: true,
                dataZoom:{
                    zoomOnMouseWheel:true,
                    filtermode: 'none'
                },
                series: [
                    {
                        type: type,
                        // roam: true,
                        symbolSize: 20,
                        roam: true,
                        zoom:0.9,
                        label: {
                            show:show_labels
                        },
        
                        emphasis:{
                            focus: 'adjacency' 
                        },
                        nodeAlign: 'right',
                        
                        data: graphData.nodeDataArray,
                        links: graphData.linkDataArray,
                        lineStyle: {
                            color: 'source',
                            curveness: 0.5
                        }
                    }
                ]
            });
    chart.on('click', function (params) {
        console.log(params, params.data);
    });
}
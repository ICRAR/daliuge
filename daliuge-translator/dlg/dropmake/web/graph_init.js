//require takes over the whole page, thus we need to load main.js with require as well
require([
    "/static/main.js",
]);
require([
    '/static/src/lib/echarts.js',
    // '/static/src/data/summit_cleaned.json'
], function (echarts) {
    var chart = echarts.init(document.getElementById('main'), {renderer:'canvas'});

    // window.onresize = function () {
    //     chart.resize();
    // };

    chart.on('click', function (params) {
        console.log(params, params.data);
    });
    $.ajax({
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
            var graphData = {'nodeDataArray':[], 'linkDataArray':[]};
            var graphDataParts = {'nodeDataArray':[], 'linkDataArray':[]};
            var newElement = {};
            data.nodeDataArray.forEach(element => {
                newElement = {};
                if (!element.hasOwnProperty("isGroup")){
                    newElement.name = element.key.toString();
                    graphData.nodeDataArray.push(newElement);
                }
                else {
                    newElement.name = element.key.toString();
                    graphDataParts.nodeDataArray.push(newElement);                  
                }
            });

            data.linkDataArray.forEach(element => {
                newElement = {};
                newElement.source = element.from.toString();
                newElement.target = element.to.toString();
                newElement.value = 20;
                graphData.linkDataArray.push(newElement);
            });

            // don't show labels if there are too many nodes. (SETTING?)
            var show_labels = (graphData.nodeDataArray.length > 750) ? false:true;

            console.log(data.nodeDataArray.Category);
            console.log(data.linkDataArray);
            console.log(graphData.nodeDataArray);
            console.log(graphData.linkDataArray);
            chart.setOption({
                tooltip: {
                    trigger: 'item',
                    triggerOn: 'mousemove'
                },
                animation: true,
                series: [
                    {
                        type: 'sankey',
                        roam: true,
                        label: {
                            show: show_labels
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
        }
    });
});
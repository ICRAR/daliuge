//require takes over the whole page, thus we need to load main.js with require as well
require([
    "/static/main.js",
]);
require([
    '/static/src/lib/echarts.js',
    // '/static/src/data/summit_cleaned.json'
], function (echarts) {
    var chart = echarts.init(document.getElementById('main'));

    window.onresize = function () {
        chart.resize();
    };

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
            data.nodeDataArray.forEach(element => {
                element.name = element.key.toString();
            });
            data.linkDataArray.forEach(element => {
                element.source = element.from.toString();
                element.target = element.to.toString();
                element.value = 20;
            });
            
            console.log(data.nodeDataArray);
            console.log(data.linkDataArray);
            data.nodeDataArray[0].itemStyle = {
            normal: {
                color: 'red'
            }
        };
        chart.setOption({
            tooltip: {
                trigger: 'item',
                triggerOn: 'mousemove'
            },
            animation: false,
            series: [
                {
                    type: 'sankey',
                    focus: 'adjacency',
                    nodeAlign: 'right',
                    animation: true,
                    data: data.nodeDataArray,
                    links: data.linkDataArray,
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
function(data){
    var graphData = {'nodeDataArray':[], 'linkDataArray':[]};
    var newElement = {};
    data.nodeDataArray.forEach(element => {
        newElement = {};
        console.log(element.hasOwnProperty("isGroup"));
        if (!element.hasOwnProperty("isGroup")){
            newElement.name = element.key.toString();
            graphData.nodeDataArray.push(newElement);
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
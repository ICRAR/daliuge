const DEFAULT_OPTIONS = Object.freeze({
    SERVER: "SERVER",
    BROWSER: "BROWSER",
    HELM: "HELM",
    OOD: "OOD"
});

const DEFAULT_URL = "http://localhost:8001/";
const DEFAULT_NAME = "default deployment";

$(document).ready(function () {
    // jquery starts here
    //hides the dropdown navbar elements when stopping hovering over the element
    $(".dropdown-menu").mouseleave(function () {
        $(".dropdown-menu").dropdown('hide')
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

    updateDeployOptionsDropdown()

    $("#aboutModal #aboutLicense").load("/static/license.html")
    //keyboard shortcuts
    const keyboardShortcuts = [];
    keyboardShortcuts.push({
        name: "Open Settings",
        shortcut: "O",
        code: 79,
        action: "$('#settingsModal').modal('toggle')"
    })
    keyboardShortcuts.push({
        name: "Deploy",
        shortcut: "D",
        code: 75,
        action: "$('#shortcutsModal').modal('toggle')"
    })
    keyboardShortcuts.push({
        name: "Open Keyboardshortcuts Modal",
        shortcut: "K",
        code: 68,
        action: "$('#activeDeployMethodButton').click()"
    })

    //fill out keyboard shortcuts modal
    keyboardShortcuts.forEach(element => {
        const shortCutItem = `<div class="col-lg-6"><div class="shortCutsModalItem"><span>${element.name}</span><span class="shortCutsModalItemRight">${element.shortcut}</span></div></div>`;
        $("#shortcutsModal .modal-body .row").append(shortCutItem)
    })

    //keyboard shortcuts execution
    $(document).keydown(function (e) {
        if ($("input").is(":focus")) {
            return
        }
        keyboardShortcuts.forEach(element => {

            if (e.which === element.code) //open settings modal on o
            {
                eval(element.action)
            }
        })
    })
});

function consoleDebugUrl(url) {
    console.debug("URL set to: ", url);
    console.debug("Protocol set to: ", url.protocol);
    console.debug("Host set to: ", url.hostname);
    console.debug("Port set to: ", url.port);
    console.debug("Prefix set to: ", url.pathname);
}

function getCurrentPageUrl() {
    const protocol = window.location.protocol;
    const host = window.location.host;
    return `${protocol}//${host}`;
}

function openSettingsModal() {
    //needed for the dropdown option to open the settings modal, the pure bootstrap method used on the settings gear button proved inconsistent
    $('#settingsModal').modal("show")
}

async function initiateDeploy(method, selected, clickedName) {
    let clickedUrl;
    JSON.parse(window.localStorage.getItem("deployMethods")).forEach(element => {
        if (element.name === clickedName) {
            clickedUrl = element.url
        }
    })

    if (selected === false) {
        await changeSelectedDeployMethod(clickedName, clickedUrl)
    }

    const activeUrlReachable = await checkUrlStatus(clickedUrl);

    if (!activeUrlReachable) {
        $("#warning-alert").fadeTo(2000, 1000).slideUp(200, function () {
            $("#warning-alert").slideUp(200);
        });
        return
    }
    if (method === DEFAULT_OPTIONS.SERVER) {
        $("#gen_pg_button").val("Generate &amp; Deploy Physical Graph")
        $("#dlg_mgr_deploy").prop("checked", true)
        $("#pg_form").submit();
    } else if (method === DEFAULT_OPTIONS.HELM) {
        $("#gen_helm_button").val("Generate &amp; Deploy Physical Graph")
        $("#dlg_helm_deploy").prop("checked", true)
        $("#pg_helm_form").submit()
    } else if (method === DEFAULT_OPTIONS.OOD) {
        await restDeploy()
    } else if (method === DEFAULT_OPTIONS.BROWSER) {
        await directRestDeploy()
    }
}

async function changeSelectedDeployMethod(name, manager_url) {
    return new Promise((resolve) => {
        const deployMethodsArray = JSON.parse(localStorage.getItem("deployMethods"));
        $("#managerUrlInput").val(manager_url);
        deployMethodsArray.forEach(element => {
            element.active = "false"
            if (element.name === name) {
                element.active = "true"
            }
        })
        window.localStorage.setItem('deployMethods', JSON.stringify(deployMethodsArray))
        updateDeployOptionsDropdown()
        resolve(true)
    })
}

function updateDeployOptionsDropdown() {
    //remove old options
    $(".deployMethodMenuItem").remove()
    let selectedUrl;

    //add deployment options
    JSON.parse(localStorage.getItem("deployMethods")).forEach(element => {
        if (element.active === "false") {
            //dropdown options
            $("#deployDropdowns .dropdown-menu").prepend(
                `<a href='javascript:void(0)' 
                onclick='initiateDeploy("${element.deployMethod}",false,"${element.name}")' 
                class='dropdown-item tooltip tooltipLeft deployMethodMenuItem' 
                data-text='Deploy Physical Graph via method:${element.deployMethod}' 
                value='Deploy Physical Graph via ${element.deployMethod}'>${element.name}</a>`
            )
        } else {
            selectedUrl = element.url
            //active option
            $("#deployDropdowns").prepend(
                `<a href='javascript:void(0)' 
                    id='activeDeployMethodButton'  
                    onclick='initiateDeploy("${element.deployMethod}",true,"${element.name}")' 
                    class='dropdown-item tooltip tooltipLeft deployMethodMenuItem' 
                    data-text='Deploy Physical Graph vi method: ${element.deployMethod} [D]' 
                    value='Deploy Physical Graph via ${element.deployMethod}'>Deploy: ${element.name}</a>`
            )
            checkActiveDeployMethod(selectedUrl)
        }


    })

    if (selectedUrl === undefined) {
        $("#deployDropdowns").prepend(
            `<a href='javascript:void(0)' 
                id='activeDeployMethodButton' 
                class='dropdown-item tooltip tooltipLeft deployMethodMenuItem' 
                data-text='No Deploy Method Selected, click to add new' 
                data-toggle='modal' 
                data-target='#settingsModal'>Add Deploy Method</a>`
        )
        return
    }

    const newUrl = new URL(selectedUrl);
    consoleDebugUrl(newUrl);
    // TODO: Why are we storing the object and then a copy of its fields?
    window.localStorage.setItem("manager_url", newUrl);
    window.localStorage.setItem("manager_protocol", newUrl.protocol);
    window.localStorage.setItem("manager_host", newUrl.host);
    window.localStorage.setItem("manager_port", newUrl.port);
    window.localStorage.setItem("manager_prefix", newUrl.pathname);
}

async function checkUrlStatus(url) {
    return new Promise((resolve) => {
        $.ajax({
            url: url + 'api',
            method: 'GET',
            mode: 'cors',
            complete: function (jqXHR, textStatus) {
                if (jqXHR.status === 200) {
                    resolve(true)
                } else {
                    console.log("Request url: " + url + "api");
                    console.log("Request status: " + jqXHR.status);
                    resolve(false);
                }
            },
            timeout: 2000
        });
    })
}

async function checkUrlSubmissionMethods(url) {
    return new Promise((resolve) => {
        $.ajax({
            credentials: 'include',
            url: url,
            type: 'GET',
            success: function (response) {
                resolve(response)
            },
            error: function (jqXHR, textStatus, errorThrown) {
                resolve({ "methods": [] })
            },
            timeout: 2000
        });
    })
}


async function manualCheckUrlStatus(clickPos) {
    let target;
    let url;
    let badUrl = false
    //if the event is triggered by click the check icon manually
    if (clickPos === "icon") {
        url = $(event.target).parent().find($('.deployMethodUrl')).val();
        target = $(event.target);
    } else {
        //if the event is triggered by focus leave on the input
        url = $(event.target).parent().parent().find($('.deployMethodUrl')).val();
        target = $(event.target).parent().parent().find($('.urlStatusIcon'));
    }


    //check if jquery deems the url constructed properly
    try {
        new URL(url);
    } catch (error) {
        badUrl = true
    }

    if (badUrl) {
        $("#settingsModalErrorMessage").html('Please ensure deploy methods URLs are valid')
    } else {
        //if the url is deemed contructed well, here we use an ajax call to see if the url is reachable
        var urlStatus = await checkUrlStatus(url)
        $("#settingsModalErrorMessage").html('')
        if (urlStatus) {
            target.replaceWith(`<div class="settingsInputTooltip tooltip tooltipBottom urlStatusIcon urlStatusReachable" data-text="Destination URL Is Reachable, click to re-check" onclick="manualCheckUrlStatus('icon')"><i class="material-icons md-24">done</i></div>`)
        } else {
            target.replaceWith(`<div class="settingsInputTooltip tooltip tooltipBottom urlStatusIcon urlStatusNotReachable" data-text="Destination URL Is Not Reachable, click to re-check" onclick="manualCheckUrlStatus('icon')"><i class="material-icons md-24">close</i></div>`)
        }
    }
}

async function checkActiveDeployMethod(url) {
    let deployButton = $("#activeDeployMethodButton");
    deployButton.removeClass("activeDeployMethodButtonOnline")
    deployButton.removeClass("activeDeployMethodButtonOffline")
    const urlStatus = await checkUrlStatus(url);
    if (urlStatus) {
        $("#activeDeployMethodButton").addClass("activeDeployMethodButtonOnline")
    } else {
        $("#activeDeployMethodButton").addClass("activeDeployMethodButtonOffline")
    }
}

function saveSettings() {
    //need a check function to confirm settings have been filled out correctly

    const settingsDeployMethods = $("#DeployMethodManager .input-group");//deploy method rows selector
    const deployMethodsArray = [];//temp array of deploy method rows values

    //errors
    let errorFillingOut = false;
    let duplicateName = false;
    let emptyName = false;
    let badUrl = false;

    settingsDeployMethods.each(function () {

        //error detection
        if ($(this).find(".deployMethodName").val().trim() === "") {
            emptyName = true
        }

        try {
            new URL($(this).find(".deployMethodUrl").val());
        } catch (error) {
            console.error("faulty Url: ", $(this).find(".deployMethodUrl").val())
            badUrl = true
        }

        //duplicate name check, the name is used as an id of sorts
        deployMethodsArray.forEach(element => {
            if ($(this).find(".deployMethodName").val() === element.name) {
                duplicateName = true
            }
        })

        //error Handling
        if (duplicateName) {
            errorFillingOut = true;
            $("#settingsModalErrorMessage").html('Please ensure there are no duplicate deploy method names')
        }
        if (emptyName) {
            errorFillingOut = true;
            $("#settingsModalErrorMessage").html('Please ensure deploy methods are named')
        }
        if (badUrl) {
            errorFillingOut = true;
            $("#settingsModalErrorMessage").html('Please ensure deploy methods URLs are valid')
        }

        let deployMethod;
        if (!errorFillingOut) {
            deployMethod =
            {
                name: $(this).find(".deployMethodName").val(),
                url: $(this).find(".deployMethodUrl").val(),
                deployMethod: $(this).find(".deployMethodMethod option:selected").val(),
                active: $(this).find(".deployMethodActive").val()
            }
            console.debug($(this).find(".deployMethodMethod option:selected").val())
            deployMethodsArray.push(deployMethod)
        }
    })

    //if errors in previous step abort saving
    if (errorFillingOut) {
        return;
    } else {
        $("#settingsModalErrorMessage").html('')
    }
    //save to local storage
    localStorage.setItem('deployMethods', JSON.stringify(deployMethodsArray))

    $('#settingsModal').modal('hide');

    //update the deploy options dropdown menu
    updateDeployOptionsDropdown()
}

function buildDeployMethodEntry(method, selected) {
    let displayValue = "";
    switch (method) {
        case DEFAULT_OPTIONS.SERVER:
            displayValue = "Server";
            break;
        case DEFAULT_OPTIONS.BROWSER:
            displayValue = "Browser Direct";
            break;
        case DEFAULT_OPTIONS.OOD:
            displayValue = "OOD";
            break;
        case DEFAULT_OPTIONS.HELM:
            displayValue = "Helm";
            break;
        default:
            displayValue = "Server";
            break;
    }
    if (selected) {
        return `<option value="${method}" selected="true">${displayValue}</option>`
    } else {
        return `<option value="${method}">${displayValue}</option>`
    }
}

function fillOutSettings() {
    let deployMethodsArray;
    //get setting values from local storage
    const manager_url = window.localStorage.getItem("manager_url");
    $("#settingsModalErrorMessage").html('')


    //fill settings with saved or default values
    if (!manager_url) {
        $("#managerUrlInput").val(DEFAULT_URL);
    } else {
        $("#managerUrlInput").val(manager_url);
    }

    //setting up initial default deploy method
    if (!localStorage.getItem("deployMethods")) {
        deployMethodsArray = [
            {
                name: DEFAULT_NAME,
                url: DEFAULT_URL,
                deployMethod: DEFAULT_OPTIONS.SERVER,
                active: true
            }
        ];
        localStorage.setItem('deployMethods', JSON.stringify(deployMethodsArray))
    } else {
        //get deploy methods from local storage 
        deployMethodsArray = JSON.parse(localStorage.getItem("deployMethods"));
    }

    //fill out settings list rom deploy methods array
    const deployMethodManagerDiv = $("#DeployMethodManager");
    deployMethodManagerDiv.empty()
    console.debug("filling out settings, GET Errors and Cors warning from Url check")
    deployMethodsArray.forEach(async element => {
        const urlReachable = await checkUrlStatus(element.url);
        const directlyAvailableMethods = await checkUrlSubmissionMethods(element.url + 'api/submission_method');
        const translatorAvailableMethods = await checkUrlSubmissionMethods(getCurrentPageUrl() + 'api/submission_method?dlg_mgr_url=' + element.url);
        const allAvailableMethods = directlyAvailableMethods["methods"].concat(translatorAvailableMethods["methods"]);
        if (allAvailableMethods.length === 0) {  // Support backend without submission/method api
            Object.keys(DEFAULT_OPTIONS).forEach(function (key) {
                allAvailableMethods.push(key);
            })
        }
        deployMethodManagerDiv.append(buildDeployMethod(allAvailableMethods, urlReachable, element.active, element.name, element.url, element.deployMethod))
    });
}

function addDeployMethod() {
    const deployMethodManagerDiv = $("#DeployMethodManager");
    let methods = [];
    Object.keys(DEFAULT_OPTIONS).forEach(function (key) {
        methods.push(key);
    })
    deployMethodManagerDiv.append(buildDeployMethod(methods, undefined, "false", "", "", undefined));
}

function buildDeployMethod(methods, reachable, deployActive, deployName, deployUrl, selectedMethod) {
    let i;
    let reachableIcon;
    if (reachable === undefined) {
        reachableIcon = `<div class="settingsInputTooltip tooltip tooltipBottom urlStatusIcon urlStatusUnknown" data-text="Destination URL status Unknown, click to check" onclick="manualCheckUrlStatus('icon')"><a class="urlStatusUnknownIcon">?</a></div>`;
    } else if (reachable) {
        reachableIcon = `<div class="settingsInputTooltip tooltip tooltipBottom urlStatusIcon urlStatusReachable" data-text="Destination URL Is Reachable, click to re-check" onclick="manualCheckUrlStatus('icon')"><i class="material-icons md-24">done</i></div>`;
    } else {
        reachableIcon = `<div class="settingsInputTooltip tooltip tooltipBottom urlStatusIcon urlStatusNotReachable" data-text="Destination URL Is Not Reachable, click to re-check" onclick="manualCheckUrlStatus('icon')"><i class="material-icons md-24">close</i></div>`;
    }
    let deployMethodRow = `<div class="input-group">
        <div class="settingsInputTooltip tooltip tooltipBottom form-control" data-text="Deploy Option Name, This must be unique"><input type="text" placeholder="Deployment Name" class=" deployMethodName" value=${deployName}></div>
        <div class="settingsInputTooltip tooltip tooltipBottom form-control" data-text="Deploy Option Destination URL"><input type="text" onfocusout="manualCheckUrlStatus('focusOut');" placeholder="Destination Url" class="deployMethodUrl" value=${deployUrl}></div>
        ${reachableIcon}
        <div class="settingsInputTooltip tooltip tooltipBottom form-control" data-text="Deploy Method"><select class="deployMethodMethod" name="Deploy Method">`;
    for (i = 0; i < methods.length; i++) {
        if (selectedMethod === undefined) {
            deployMethodRow += buildDeployMethodEntry(methods[i], i === 0);
        } else if (selectedMethod === methods[i].toString()) {
            deployMethodRow += buildDeployMethodEntry(methods[i], true);
        } else {
            // Has the edge case resulting in nothing being selected if the previous option is no longer available. I think this is an acceptable interpretation @pritchardn
            deployMethodRow += buildDeployMethodEntry(methods[i], false);
        }
    }
    deployMethodRow +=
        `</select></div>
            <input type="text" class="form-control deployMethodActive" value=${deployActive}>
            <button class="btn btn-secondary btn-sm tooltip tooltipBottom" data-text="Delete Deploy Option" type="button" onclick="removeDeployMethod(event)"><i class="material-icons md-24">delete</i></button>
            </div>`
    return deployMethodRow;
}

function removeDeployMethod(e) {
    $(e.target).parent().remove()
}

function makeJSON() {
    console.info("makeJSON()");

    $.ajax({
        url: "/pgt_jsonbody?pgt_name=" + pgtName,
        type: 'get',
        error: function (XMLHttpRequest) {
            if (404 === XMLHttpRequest.status) {
                console.error('Server cannot locate physical graph file ', pgtName);
            } else {
                console.error(`status: ${XMLHttpRequest.status}, status text: ${XMLHttpRequest.statusText}`);
            }
        },
        success: function (data) {
            downloadText(pgtName, data);
        }
    });
}

function makePNG() {

    html2canvas(document.querySelector("#main")).then(canvas => {
        const dataURL = canvas.toDataURL("image/png");
        const data = atob(dataURL.substring("data:image/png;base64,".length)),
            asArray = new Uint8Array(data.length);

        let i = 0, len = data.length;
        for (; i < len; ++i) {
            asArray[i] = data.charCodeAt(i);
        }

        const blob = new Blob([asArray.buffer], { type: "image/png" });
        saveAs(blob, pgtName + "_Template.png");
    });
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

function handleFetchErrors(response) {
    if (!response.ok) {
        throw Error(response.statusText);
    }
    return response;
}

async function directRestDeploy() {

    // fetch manager host and port from local storage
    let murl = window.localStorage.getItem("manager_url");
    if (!murl) {
        saveSettings();
        $('#settingsModal').modal('show');
        $('#settingsModal').on('hidden.bs.modal', function () {
            fillOutSettings()
            murl = window.localStorage.getItem("manager_url");
        })
    }

    let manager_url = new URL(murl);
    console.info("In Direct REST Deploy");

    const manager_host = manager_url.hostname;
    const request_mode = "cors";
    const pgt_id = $("#pg_form input[name='pgt_id']").val();
    manager_url = manager_url.toString();
    if (manager_url.endsWith('/')) {
        manager_url = manager_url.substring(0, manager_url.length - 1);
    }
    consoleDebugUrl(manager_url);

    // sessionId must be unique or the request will fail
    const lgName = pgtName.substring(0, pgtName.lastIndexOf("_pgt.graph"));
    const dateId = new Date();
    const sessionId = lgName + "-" + dateId.toISOString().replace(/\:/gi, "-");
    console.debug("sessionId:", sessionId);

    const nodes_url = manager_url + "/api/nodes";

    const nodes = await fetch(nodes_url, {
        method: 'GET',
        // mode: request_mode
    })
        .then(handleFetchErrors)
        .then(response => response.json())
        .catch(function (error) {
            showMessageModal(`Error ${error}! Getting nodes unsuccessful`);
        })

    const pgt_url = "/gen_pg?tpl_nodes_len=" + nodes.length.toString() + "&pgt_id=" + pgtName;
    console.debug("sending request to ", pgt_url);
    console.debug("graph name: ", pgtName);
    await fetch(pgt_url, {
        method: 'GET',
    })
        .then(handleFetchErrors)
        .then(response => response.json())
        .catch(function (error) {
            showMessageModal(`Error ${error}\nGetting PGT unsuccessful: Unable to continue!`);
        });

    console.debug("node_list", nodes);
    const pg_spec_request_data = {
        manager_host: manager_host,
        node_list: nodes,
        pgt_id: pgt_id
    }

    // request pg_spec from translator
    const pg_spec_url = "/gen_pg_spec";
    console.debug("pg_spec_request", pg_spec_request_data);
    const pg_spec_response = await fetch(pg_spec_url, {
        method: 'POST',
        mode: 'cors',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(pg_spec_request_data)
    })
        .then(handleFetchErrors)
        .then(response => response.json())
        .catch(function (error) {
            showMessageModal('Error', error + "\nGetting pg_spec unsuccessful: Unable to continue!");
        });
    const session_data = { "sessionId": sessionId };
    console.debug("SessionId:", sessionId);
    const create_session_url = manager_url + "/api/sessions";
    const create_session = await fetch(create_session_url, {
        credentials: 'include',
        cache: 'no-cache',
        method: 'POST',
        // mode: request_mode,
        referrerPolicy: 'no-referrer',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(session_data)
    })
        .then(handleFetchErrors)
        .then(response => response.json())
        .catch(function (error) {
            showMessageModal('Error', error + "\nCreating session unsuccessful: Unable to continue!");
        });
    console.debug("create session response", create_session);
    // gzip the pg_spec
    const buf = fflate.strToU8(JSON.stringify(JSON.parse(pg_spec_response).pg_spec));
    const compressed_pg_spec = fflate.zlibSync(buf);
    // console.debug("compressed_pg_spec", compressed_pg_spec);
    // console.debug("pg_spec", compressed_pg_spec);

    // append graph to session on engine
    const append_graph_url = manager_url + "/api/sessions/" + sessionId + "/graph/append";
    const append_graph = await fetch(append_graph_url, {
        credentials: 'include',
        method: 'POST',
        mode: "no-cors",
        referrerPolicy: 'origin',
        // body: JSON.stringify(pg_spec_response.pg_spec)
        // body: new Blob(compressed_pg_spec, { type: 'application/json' })
        body: new Blob([buf], { type: 'application/json' })
    })
    const deploy_graph_url = manager_url + "/api/sessions/" + sessionId + "/deploy";
    const deploy_graph = await fetch(deploy_graph_url, {
        // credentials: 'include',
        method: 'POST',
        mode: "no-cors",
        body: new URLSearchParams({
            'completed': JSON.parse(pg_spec_response).root_uids,
        })
    })
    const mgr_url = manager_url + "/session?sessionId=" + sessionId;
    window.open(mgr_url, 'deploy_target').focus();
}

function jsonEscape(str) {
    return str.replace(/\n/g, "\\\\n").replace(/\r/g, "\\\\r").replace(/\t/g, "\\\\t");
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
    }
    let manager_url = new URL(murl);
    console.info("In REST Deploy")

    const request_mode = "no-cors";
    manager_url = manager_url.toString();
    if (manager_url.endsWith('/')) {
        manager_url = manager_url.substring(0, manager_url.length - 1);
    }
    consoleDebugUrl(manager_url);

    // sessionId must be unique or the request will fail
    const lgName = pgtName.substring(0, pgtName.lastIndexOf("_pgt.graph"));
    const sessionId = `${lgName}-${Date.now()}`;
    console.debug("sessionId: ", sessionId);

    // build urls
    // the manager_url in this case has to point to daliuge_ood
    const create_slurm_url = manager_url + "/api/slurm/script";
    const pgt_url = "/gen_pg?tpl_nodes_len=1&pgt_id=" + pgtName; // TODO: tpl_nodes_len >= nodes in LG

    // fetch the PGT from this server
    console.debug("sending request to ", pgt_url);
    console.debug("graph name:", pgtName);
    let pgt = await fetch(pgt_url, {
        method: 'GET',
    })
        .then(handleFetchErrors)
        .then(response => response.json())
        .catch(function (error) {
            showMessageModal('Error', error + "\nGetting PGT unsuccessful: Unable to continue!");
        });
    if (typeof pgt == "String") {
        pgt = JSON.parse(jsonEscape(toString(pgt)));
    }
    // This is for a deferred start of daliuge, e.g. on SLURM
    var body = [pgtName, pgt]; // we send the name in the body with the pgt
    console.debug("Submission PGT:", JSON.stringify(body));
    console.debug("sending request to ", create_slurm_url);
    await fetch(create_slurm_url, {
        method: 'POST',
        credentials: 'include',
        cache: 'no-cache',
        mode: "cors",
        referrerPolicy: 'no-referrer',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
    })
        .then(handleFetchErrors)
        .then(response => {
            console.log(response);
            console.log(response.url);
            window.open(response.url, 'deploy_target').focus();
        })
        .catch(function (error) {
            console.log(error);
            // window.open("https://ood.icrar.org/pun/sys/dashboard/activejobs", 'deploy_target').focus();
            showMessageModal(`Error ${error}\nSending PGT to backend unsuccessful!`);
        });
}

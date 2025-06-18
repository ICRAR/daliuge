//
//    ICRAR - International Centre for Radio Astronomy Research
//    (c) UWA - The University of Western Australia, 2015
//    Copyright by UWA (in the framework of the ICRAR)
//    All rights reserved
//
//    This library is free software; you can redistribute it and/or
//    modify it under the terms of the GNU Lesser General Public
//    License as published by the Free Software Foundation; either
//    version 2.1 of the License, or (at your option) any later version.
//
//    This library is distributed in the hope that it will be useful,
//    but WITHOUT ANY WARRANTY; without even the implied warranty of
//    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
//    Lesser General Public License for more details.
//
//    You should have received a copy of the GNU Lesser General Public
//    License along with this library; if not, write to the Free Software
//    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
//    MA 02111-1307  USA
//

var SESSION_STATUS = ['Pristine', 'Building', 'Deploying', 'Running', 'Finished', 'Cancelled']
var STATUS_CLASSES = ['initialized', 'writing', 'completed', 'error', 'expired', 'deleted', 'cancelled', 'skipped']
var EXECSTATUS_CLASSES = ['not_run', 'running', 'finished', 'error', 'cancelled', 'skipped']
var TYPE_CLASSES = ['app', 'container', 'socket', 'plain']
var TYPE_SHAPES = { app: 'rect', container: 'parallelogram', socket: 'parallelogram', data: 'parallelogram' }

var TO_MANY_LTR_RELS = ['consumers', 'streamingConsumers', 'outputs']
var TO_MANY_RTL_RELS = ['inputs', 'streamingInputs', 'producers']

function get_status_name(s) {
	if (typeof s.execStatus != 'undefined') {
		return EXECSTATUS_CLASSES[s.execStatus];
	} else {
		return STATUS_CLASSES[s.status];
	}
}

function uniqueSessionStatus(status) {

	// If we are querying one of the Composite Managers (like the DIM or the MM)
	// we need to reduce the individual session status to a single one for display
	if (status != null && typeof status === 'object') {

		// Reduce, reduce, reduce
		while (true) {

			// Get the values from the status object
			status = Object.keys(status).map(function (k) {
				return status[k]
			});

			// If the values in the resulting array are not objects then
			// we already hit the bottom level and we have simply numbers
			// in the array
			if (typeof status[0] !== 'object') {
				break;
			}

			// Otherwise, we create an object which consists on the merged
			// objects contained in the array
			// e.g., [{a:'b'}, {b:'c'}] -> {a:'b', b:'c'}
			// After that we're OK for the next iteration
			status = status.reduce(
				function (prev, v, idx, array) {
					if (idx == 0) {
						return v;
					}
					for (var attrname in prev) {
						v[attrname] = prev[attrname];
					}
					return v;
				}
			)
		}

		// Reduce to single common value if possible
		// "Finished" and "Running" reduce to "Running"
		// Otherwise we reduce to -1, which we interpret as "Indeterminate"
		return status.reduce(
			function (prev, v, idx, array) {
				if (prev == -1) {
					return -1;
				} else if (prev == 3 && v == 4 || prev == 4 && v == 3) {
					return 3;
				}
				return (prev == v) ? v : -1;
			}
		);
	}

	// otherwise we simply return the status, which should be an integer
	return status;
}

function sessionStatusToString(status) {
	return (status == -1) ? 'Indeterminate' : SESSION_STATUS[status];
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
			.attr("points", points.map(function (d) {
				return d.x + "," + d.y;
			}).join(" "))
			.attr("transform", "translate(" + (-w / 2) + "," + (h / 2) + ")");

		node.intersect = function (point) {
			return dagreD3.intersect.polygon(node, points, point);
		};

		return shapeSvg;
	};
	console.log("returning render")
	return render;
}

function loadSessions(serverUrl, tbodyEl, refreshBtn, selectedNode, delay) {

	// console.log("Inside loadSessions");
	refreshBtn.attr('disabled');
	// Support for node query forwarding
	var url = serverUrl + '/api';
	if (selectedNode) {
		url += '/node/' + selectedNode;
	}
	url += '/sessions';

	var sessionLink = function (s) {
		var url = 'session?sessionId=' + s;
		if (selectedNode) { url += '&node=' + selectedNode; }
		return url;
	};

	var DimSessionLink = function (s) {
		var url = 'session?sessionId=' + s;
		if (selectedNode) { url += '&node=' + selectedNode; }
		var dimUrlQuery = new URL(window.location.href);
		var dimUrl = dimUrlQuery.searchParams.get("dim_url");
		if (dimUrl) {
			url += "&dim_url=" + dimUrl;
		}
		return url;
	};

	var cancelBtnSessionId = function (s) {
		// console.log(hashCode(s))
		return "cancelBtn" + hashCode(s);
	};

	var deleteBtnSessionId = function (s) {
		// console.log(hashCode(s))
		return "deleteBtn" + hashCode(s);
	};

	var hashCode = function (s) {
		return s.split("").reduce(function (a, b) { a = ((a << 5) - a) + b.charCodeAt(0); return a & a }, 0);
	}

	d3.json(url).then(function (response, error) {

		if (error) {
			console.error(error)
			refreshBtn.attr('disabled', null);
			return
		}
		var sessions = response;
		sessions.sort(function comp(a, b) {
			return (a.sessionId > b.sessionId) ? -1 : (a.sessionId < b.sessionId);
		});
		var rows = tbodyEl.selectAll('tr').data(sessions);
		rows.exit().remove();
		rows.enter().append('tr');
		rows.exit().transition().delay(0).duration(500).style('opacity', 0.0).remove();
		rows.enter().append('tr').style('opacity', 0.0).transition().delay(0).duration(500).style('opacity', 1.0);

		fillDmTable(sessions, tbodyEl, sessionLink, DimSessionLink, cancelBtnSessionId, deleteBtnSessionId, hashCode);
		//progressbars in dim

		const width = $('#sessionsTable').find('.status').innerWidth();


		var graph_update_handler = function (oids, dropSpecs) { };

		var status_update_handler = function (statuses) {
			var states = ['completed', 'finished',
				'running', 'writing',
				'error', 'expired', 'deleted',
				'cancelled', 'skipped',
				'not_run', 'initialized'];
			var states_idx = d3.scalePoint().domain(states).range([0, states.length - 1]);

			var scale = function (x) {
				return Math.round(x * width / statuses.length);
			};

			/* Get total and per-status counts, then normalize to 0-100% */
			var total = statuses.length;
			var status_counts = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
			statuses.reduce(function (status_counts, s) {
				var idx = states_idx(get_status_name(s));
				status_counts[idx] = status_counts[idx] + 1;
				return status_counts;
			}, status_counts);

			for (var cumsum = [0], i = 0; i < status_counts.length - 1; i++)
				cumsum[i + 1] = cumsum[i] + status_counts[i];

			status_counts = status_counts.map(function (x, i) {

				return [scale(cumsum[i]), scale(x)];
			});
			var rects = d3.select('#sessionsTable .status svg').selectAll('rect').data(status_counts);
			rects.enter().append('rect')
				.style('height', 20).style('width', 0).style('x', 0).style('y', 0)
				// .transition().delay(0).duration(500)
				.style('x', function (d) { return d[0] + 20; })
				.style('width', function (d) { return d[1]; })
				.attr('class', function (d) { return states[status_counts.indexOf(d)]; });
			rects.style('x', function (d) { return d[0] + 20; })
				.style('width', function (d) { return d[1]; })
				.attr('class', function (d) { return states[status_counts.indexOf(d)]; });
			rects.exit().remove();
		};


		//update status colours and hide cancel button if finished or cancelled
		$(".status").each(function () {
			var currentStatus = $(this).html()

			if (currentStatus === "Cancelled") {
				$(this).css("color", "grey");
				$(this).parent().find(".actions").find("button.cancelSession").attr("disabled", true)
				$(this).parent().find(".actions").find("button.deleteSession").attr("disabled", false)
				$(this).parent().find(".actions").find("button.sessionLogs").attr("disabled", false)
				$(this).parent().removeClass("progressRunning")
			} else if (currentStatus === "Deploying") {
				$(this).css("color", "blue");
				$(this).parent().removeClass("progressRunning")
				$(this).parent().find(".actions").find("button.cancelSession").attr("disabled", false)
				$(this).parent().find(".actions").find("button.deleteSession").attr("disabled", true)
				$(this).parent().find(".actions").find("button.sessionLogs").attr("disabled", true)
			}
			else if (currentStatus === "Running") {
				$(this).text("");
				$(this).parent().find(".actions").find("button.cancelSession").attr("disabled", false)
				$(this).parent().find(".actions").find("button.deleteSession").attr("disabled", true)
				$(this).parent().find(".actions").find("button.sessionLogs").attr("disabled", true)
				$(this).append("<svg>")
				if (!$(this).parent().hasClass('progressRunning')) {
					startStatusQuery(serverUrl, $(this).parent().find(".id").text(), selectedNode, graph_update_handler,
						status_update_handler, 1000);
					$(this).parent().addClass("progressRunning")
				}
			} else if (currentStatus === "Finished") {
				$(this).css("color", "#00af28");
				$(this).parent().find(".actions").find("button.cancelSession").attr("disabled", true)
				$(this).parent().find(".actions").find("button.deleteSession").attr("disabled", false)
				$(this).parent().find(".actions").find("button.sessionLogs").attr("disabled", false)
				$(this).parent().removeClass("progressRunning")
			} else if (currentStatus === "Pristine") {
				$(this).css("color", "#b93a46");
				$(this).parent().find(".actions").find("button.cancelSession").attr("disabled", true)
				$(this).parent().find(".actions").find("button.deleteSession").attr("disabled", false)
				$(this).parent().find(".actions").find("button.sessionLogs").attr("disabled", true)
				$(this).parent().removeClass("progressRunning")
			} else {
				$(this).css("color", "purple");
				$(this).parent().find(".actions").find("button.cancelSession").attr("disabled", true)
				$(this).parent().find(".actions").find("button.deleteSession").attr("disabled", false)
				$(this).parent().find(".actions").find("button.sessionLogs").attr("disabled", false)
				$(this).parent().removeClass("progressRunning")
			}
		})

		refreshBtn.attr('disabled', null);

		if (!(typeof delay === 'undefined')) {
			var loadSessionTimer = d3.timer(function () {
				loadSessions(serverUrl, tbodyEl, refreshBtn, selectedNode, delay);
				loadSessionTimer.stop()
				return;
			}, delay);
		}
	});
}


function loadPastSessions(serverUrl, tbodyEl, refreshBtn, selectedNode, delay) {

	// console.log("Inside loadSessions");
	refreshBtn.attr('disabled');
	// Support for node query forwarding
	var url = serverUrl + '/api';
	if (selectedNode) {
		url += '/node/' + selectedNode;
	}
	url += '/past_sessions';

	var sessionLink = null;

	var DimSessionLink = function (s) {
		var url = 'session?sessionId=' + s;
		if (selectedNode) { url += '&node=' + selectedNode; }
		var dimUrlQuery = new URL(window.location.href);
		// Temporarily redirect back to DIM window until we provide 
		// past session information. 
		return dimUrlQuery;	
	};

	d3.json(url).then(function (response, error) {

		if (error) {
			console.error(error)
			refreshBtn.attr('disabled', null);
			return
		}
		var sessions = response;
		sessions.sort(function comp(a, b) {
			return (a.sessionId > b.sessionId) ? -1 : (a.sessionId < b.sessionId);
		});
		var rows = tbodyEl.selectAll('tr').data(sessions);
		rows.exit().remove();
		rows.enter().append('tr');
		rows.exit().transition().delay(0).duration(500).style('opacity', 0.0).remove();
		rows.enter().append('tr').style('opacity', 0.0).transition().delay(0).duration(500).style('opacity', 1.0);

		fillDmTable(sessions, tbodyEl, sessionLink, DimSessionLink, null, null, false, false);
		//progressbars in dim

		// const width = $('#pastSessionsTable').find('.status').innerWidth();

		refreshBtn.attr('disabled', null);

		if (!(typeof delay === 'undefined')) {
			var loadPassSessionTimer = d3.timer(function () {
				loadPastSessions(serverUrl, tbodyEl, refreshBtn, selectedNode, delay);
				loadPastSessionTimer.stop()
				return;
			}, delay);
		}
	});

}


function fillDmTable(
	sessions,
	tbodyEl,
	sessionLink,
	DimSessionLink,
	cancelBtnSessionId,
	deleteBtnSessionId,
	displayStatus=true,
	displaySize=true,
	hashCode) {
	var rows = tbodyEl.selectAll('tr').data(sessions);
	var idCells = rows.selectAll('td.id').data(function values(s) { return [s.sessionId]; });
	idCells.enter().append('td').classed('id', true).text(String)
	idCells.text(String)
	idCells.exit().remove()

	if (displayStatus)
	{
		var statusCells = rows.selectAll('td.status').data(function values(s) { return [uniqueSessionStatus(s.status)]; });
		statusCells.enter().append('td').classed('status', true).text(function (s) { return sessionStatusToString(s); })
		statusCells.text(function (s) { return sessionStatusToString(s) })
		statusCells.exit().remove()
	}
	if (displaySize)
	{
		var sizeCells = rows.selectAll('td.size').data(function values(s) { return [s.size]; });
		sizeCells.enter().append('td').classed('size', true).text(String)
		sizeCells.text(String)
		sizeCells.exit().remove()
	}

	statusCells = rows.selectAll('td.details').data(function values(s) { return [s.sessionId]; });
	statusCells.enter().append('td').classed('details', true)
		.append('a').attr('href', DimSessionLink)
		.append('span').classed('fa fa-share', true)
	statusCells.select('a').attr('href', DimSessionLink)
	statusCells.exit().remove()

	var actionCells = rows.selectAll('td.actions').data(function values(s) { return [s.sessionId]; });
	if (cancelBtnSessionId != null && deleteBtnSessionId != null)
	{
		actionCells.enter().append('td').classed('actions', true)
			// .html('<button id="'+cancelBtnSessionId+'"class="btn btn-secondary" type="button" onclick="cancel_session(serverUrl,"false",this.id)">cancel</button>')
			// .html('<button id="'+deleteBtnSessionId+'"class="btn btn-secondary" type="button" onclick="cancel_session(serverUrl,"false",this.id)">delete</button>')
			.append("button").attr('id', cancelBtnSessionId)
			.attr("type", 'button').attr('class', 'btn btn-secondary cancelSession fa fa-ban').attr('onclick', '(cancel_session(serverUrl,"false",this.id))')
			.attr('data-bs-toggle', 'tooltip').attr('data-bs-placement', 'bottom').attr('title', 'cancel ongoing session')
			.select(function () { return this.parentNode.appendChild(this.cloneNode(true)); })
			.attr('id', deleteBtnSessionId)
			.attr("type", 'button').attr('class', 'btn btn-secondary deleteSession fa fa-trash').attr('onclick', '(delete_session(serverUrl,"false",this.id))')
			.attr('data-bs-toggle', 'tooltip').attr('data-bs-placement', 'bottom').attr('title', 'Delete session')
		//log button ready for linking
		// .select(function() { return this.parentNode.appendChild(this.cloneNode(true)); })
		// .attr('id', "logs")
		// .attr("type", 'button').attr('class', 'btn btn-secondary sessionLogs fa fa-file-text').attr('onclick', '(delete_session(serverUrl,"false",this.id))')
		// .attr( 'data-bs-toggle','tooltip').attr('data-bs-placement','bottom').attr('title','Show session logs')
		actionCells.selectAll('button')
		actionCells.exit().remove()
	}

	$("button").tooltip({

		boundary: 'window',
		trigger: 'hover',
		delay: { "show": 800, "hide": 100 }
	});
}

function handleFetchErrors(response) {
	if (!response.ok) {
		throw Error(response.statusText);
	}
	return response;
}

function promptNewSession(serverUrl, tbodyEl, refreshBtn) {
	bootbox.prompt("Session ID", function (sessionId) {
		if (sessionId == null) {
			return;
		}
		fetch(serverUrl + '/api/sessions', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
			},
			body: JSON.stringify({ sessionId: sessionId })
		})
			.then(handleFetchErrors)
			.then(function (response) {
				response => response.json()
				loadSessions(serverUrl, tbodyEl, refreshBtn, null)
			})
			.catch(function (error) {
				console.error(error)
				bootbox.alert('An error occurred while creating session ' + sessionId + ': ' + error.responseText)
				return
			});
	});
}

function drawGraphForDrops(g, drawGraph, oids, doSpecs, url) {

	// Keep track of modifications to see if we need to re-draw
	var modified = false;

	// #1: create missing nodes in the graph
	// Because oids is sorted, they will be created in oid order
	var time0 = new Date().getTime();
	for (var idx in oids) {
		if (oids[idx] != 'reprodata') {
			var doSpec = doSpecs[oids[idx]];
			modified |= _addNode(g, doSpec, url);
		}
	}

	var time1 = new Date().getTime();
	console.log('Took %d [ms] to create the nodes', (time1 - time0))

	// #2: establish missing relationships
	console.log(doSpecs)
	for (var idx in oids) {

		var doSpec = doSpecs[oids[idx]];
		var lhOid = doSpec.oid;

		// x-to-many relationships producing lh->rh edges
		for (var relIdx in TO_MANY_LTR_RELS) {
			var rel = TO_MANY_LTR_RELS[relIdx];
			if (rel in doSpec) {
				for (var rhOid in doSpec[rel]) {
					if (rhOid.constructor == Object) {
						rhOid = Object.keys(rhOid)[0]
					} modified |= _addEdge(g, lhOid, doSpec[rel][rhOid]);
				}
			}
		}
		// x-to-many relationships producing rh->lh edges
		for (var relIdx in TO_MANY_RTL_RELS) {
			var rel = TO_MANY_RTL_RELS[relIdx];
			if (rel in doSpec) {
				for (var rhOid in doSpec[rel]) {
					modified |= _addEdge(g, doSpec[rel][rhOid], lhOid);
				}
			}
		}
		// there currently are no x-to-one relationships producing rh->lh edges
		// there currently are no x-to-one relationships producing lh->rh edges
	}

	var time2 = new Date().getTime();
	console.log('Took %d [ms] to create the edges', (time2 - time1))

	if (modified) {
		drawGraph();
		zoomFit();
	}

	var time3 = new Date().getTime();
	console.log('Took %d [ms] to draw the hole thing', (time3 - time2))

}

function setStatusColor(status) {
	if (status === "Cancelled") {
		$("#session-status").css("color", "grey");
		$("#cancelBtn").hide();
	} else if (status === "Running") {
		$("#session-status").css("color", "#ecde7b");
	} else {
		$("#session-status").css("color", "lime");
		$("#cancelBtn").hide();
	}
}

/**
 * Starts a regular background task that retrieves the current graph
 * specification from the REST server until the session's status is either
 * DEPLOYING or RUNNING, in which case no more graph structure changes are
 * expected.
 *
 * Using the graph specification received from the server the given callback
 * is invoked, which may use the information as necessary.
 *
 * Once the status of the session is RUNNING or DEPLOYING, this task should not
 * scheduled anymore, and #startGraphStatusUpdates is called instead.
 *
 */
function startStatusQuery(serverUrl, sessionId, selectedNode, graph_update_handler,
	status_update_handler, delay) {
	// Support for node query forwarding
	var url = serverUrl + '/api';
	if (selectedNode) {
		url += '/node/' + selectedNode;
	}
	url += '/sessions/' + sessionId;
	var updateGraphDelayTimerActive = false;
	var updateGraphDelayTimer;

	function updateGraph() {
		d3.json(url).then(function (sessionInfo, error) {
			if (error) {
				console.log("error")
				console.error(error);
				return;
			}

			var doSpecs = sessionInfo['graph'];
			var status = uniqueSessionStatus(sessionInfo['status']);
			d3.select('#session-status').text(sessionStatusToString(status));
			setStatusColor(sessionStatusToString(status));

			var oids = Object.keys(doSpecs);
			if (oids.length > 0) {
				// Get sorted oids
				oids.sort();
				graph_update_handler(oids, doSpecs, url);
			}

			// During PRISITINE and BUILDING we need to update the graph structure
			// During DEPLOYING we call ourselves again anyway, because we need
			// to know when we go to RUNNING.
			// During RUNNING (or potentially FINISHED/CANCELLED, if the execution is
			// extremely fast) we need to start updating the status of the graph
			if (status === 3 || status === 4 || status === 5) {
				startGraphStatusUpdates(serverUrl, sessionId, selectedNode, delay,
					status_update_handler);
			}
			else if (status === 0 || status === 1 || status === 2 || status === -1) {
				if (status === 2) {
					// Visualise the drops if we are trying to 'deploy' them.
					var keys = Object.keys(doSpecs);
					keys.sort();
					var statuses = keys.map(function (k) { return {"status": 0} });
					status_update_handler(statuses);
				}
				// schedule a new JSON request
				updateGraphDelayTimer = d3.timer(updateGraph, delay);
				updateGraphDelayTimerActive = true;
			}

		})
		// This makes d3.timer invoke us only once
		// return true;
		if (updateGraphDelayTimerActive === true) {
			updateGraphDelayTimer.stop();
			updateGraphDelayTimerActive = false;
		};
		updateGraphTimer.stop();
		return;
	}
	var updateGraphTimer = d3.timer(updateGraph);
}

function _addNode(g, doSpec, url) {

	var oid = doSpec.oid;

	if (g.hasNode(oid)) {
		return false;
	}

	var typeClass = doSpec.type;
	var typeShape = TYPE_SHAPES[doSpec.type];
	var notes = '';
	// console.log('Drop type', doSpec.type)
	if (doSpec.name) {
		notes = "<span>" + doSpec.name + "</span>"
	}
	if (doSpec.type == 'app') {
		var nameParts = doSpec.app.split('.');
		notes += nameParts[nameParts.length - 1];
	}
	else if (doSpec.type == 'socket') {
		notes += 'port: ' + doSpec.port;
	}
	else if (doSpec.type == 'plain') {
		notes += 'storage: ' + doSpec.storage;
	}
    url = url.replace("api/","") + "/graph/drop/" +  doSpec.oid;
    let link = "<a href=" + url +  " target='_blank'>Details</a>";
	var html = '<div class="drop-label ' + typeShape + '" id="id_' + oid + '">';
	html += '<span class="notes">' + notes + '</span>';
    oid_date = doSpec.oid.split("_")[0];
	human_readable_id = oid_date + "_" + doSpec.humanReadableKey.toString()
	html += '<span style="font-size: 13px;">' + human_readable_id + '</span>';
	if (doSpec.categoryType != "Data") {
	    html += '<span style="font-size: 13px;">' + link + '</span>';
	}
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

function _addEdge(g, fromOid, toOid) {
	if (fromOid.constructor == Object) {
		fromOid = Object.keys(fromOid)[0]
	}
	if (toOid.constructor == Object) {
		toOid = Object.keys(toOid)[0]
	} if (g.hasEdge(fromOid, toOid)) {
		return false;
	}
	if (!g.hasNode(fromOid)) {
		console.error('No DROP found with oid ' + fromOid);
		return false;
	}
	if (!g.hasNode(toOid)) {
		console.error('No DROP found with oid ' + toOid);
		return false;
	}
	g.setEdge(fromOid, toOid, { width: 40 });
	return true;
}


/**
 * Starts a regular background task that retrieves the current status of the
 * graph from the REST server, updating the current display to show the correct
 * colors
 */
function startGraphStatusUpdates(serverUrl, sessionId, selectedNode, delay,
	status_update_handler) {
	// Support for node query forwarding
	var url = serverUrl + '/api';
	if (selectedNode) {
		url += '/node/' + selectedNode;
	}
	url += '/sessions/' + sessionId + '/graph/status';
	var updateStatesDelayTimerActive = false;
	var updateStatesDelayTimer;

	function updateStates() {
		d3.json(url).then(function (response, error) {
			if (error) {
				console.error(error);
				return;
			}
			// Change from {B:{status:2,execStatus:0}, A:{status:1}, ...}
			//          to [{status:1},{status:2,execStatus:0}...]
			// (i.e., sort by key and get values only)
			var keys = Object.keys(response);
			keys.sort();
			var statuses = keys.map(function (k) { return response[k] });
			// console.log(statuses)
			// This works assuming that the status list comes in the same order
			// that the graph was created, which is true
			// Anyway, we could double-check in the future
			status_update_handler(statuses);

			var allCompleted = statuses.reduce(function (prevVal, curVal, idx, arr) {
				var cur_status = get_status_name(curVal);
				return prevVal && (cur_status == 'completed' || cur_status == 'finished' || cur_status == 'error' || cur_status == 'cancelled' || cur_status == 'skipped' || cur_status == 'deleted' || cur_status == 'expired');
			}, true);
			if (!allCompleted) {
				updateStatesDelayTimer = d3.timer(updateStates, delay);
				updateStatesDelayTimerActive = true
			}
			else {
				// A final update on the session's status
				d3.json(serverUrl + '/api/sessions/' + sessionId + '/status').then(function (status, error) {
					if (error) {
						console.error(error);
						return;
					}
					d3.select('#session-status').text(sessionStatusToString(uniqueSessionStatus(status)));
					setStatusColor(sessionStatusToString(uniqueSessionStatus(status)));
				});
			}
		})

		if (updateStatesDelayTimerActive === true) {
			updateStatesDelayTimer.stop();
			updateStatesDelayTimerActive = false;
		};
		stateUpdateTimer.stop();
		return;
	}
	var stateUpdateTimer = d3.timer(updateStates);
}

/**
 * Determine, based on the numerical status, if the associated session can be cancelled.
 *
 * @param status  the numerical status of the associated session.
 * @returns {boolean}  true if it can be cancelled and false otherwise.
 */
function does_status_allow_cancel(status) {
	// During RUNNING we can cancel
	if (uniqueSessionStatus(status) == 3) {
		return true;
	} else {
		return false;
	}
}

function sessionId_from_buttonId(buttonId) {
	//getting session id from sibling in table using js
	button = "#" + buttonId
	sessionId = $(button).parent().parent().find("td.details").find('a').attr("href")
	sessionId = sessionId.split("=")
	sessionId = sessionId[1].split("&")
	sessionId = sessionId[0]
	return sessionId
}

/**
 * Cancel the given sessionId using the provided information.
 *
 * @param serverUrl to use for the REST API
 * @param sessionId to cancel
 * @param cancelSessionBtn that initiated the cancel
 */
//  function cancel_session(serverUrl, sessionId, cancelSessionBtn) {
function cancel_session(serverUrl, sessionId, buttonId) {
	if (sessionId === "false") {
		//getting session id from sibling in table using js
		button = "#" + buttonId
		sessionId = sessionId_from_buttonId(buttonId)
		cancelSessionBtn = $(button)
	} else {
		cancelSessionBtn = buttonId
	}

	var url = serverUrl + '/api';
	url += '/sessions/' + sessionId;

	d3.json(url).then(function (sessionInfo, error) {

		if (error) {
			//bootbox.alert(error);
			console.error(error);
			return;
		}

		if (does_status_allow_cancel(sessionInfo['status'])) {
			bootbox.alert("Cancel of " + sessionId + " in progress.");
			url += '/cancel';
			cancelSessionBtn.attr('disabled', null);

			d3.json(url, {
				method: 'POST',
				headers: {
					"Content-type": "application/json; charset=UTF-8"
				},
				body: JSON.stringify(function (response, error) {
					// We don't expect a response so ignoring it.

					if (error) {
						console.log(response)
						console.error(error)
						return
					}

					cancelSessionBtn.attr('disabled', null);
				})
			});

			d3.select('#session-status').text("Cancelled");
			setStatusColor("Cancelled");
		} else {
			// display an error
			bootbox.alert("Can't cancel " + sessionId + " unless it is RUNNING.");
		}
	})
}

/**
 * Delete the given sessionId using the provided information.
 *
 * @param serverUrl to use for the REST API
 * @param sessionId to delete
 * @param deleteSessionBtn that initiated the delete
 */
//  function delete_session(serverUrl, sessionId, deleteSessionBtn) {
function delete_session(serverUrl, sessionId, buttonId) {
	if (sessionId === "false") {
		//getting session id from sibling in table using js
		button = "#" + buttonId
		sessionId = sessionId_from_buttonId(buttonId)
		deleteSessionBtn = $(button)
	} else {
		deleteSessionBtn = buttonId
	}

	var url = serverUrl + '/api';
	url += '/sessions/' + sessionId;

	d3.json(url).then(function (sessionInfo, error) {

		if (error) {
			//bootbox.alert(error);
			console.error(error);
			return;
		}

		if (!does_status_allow_cancel(sessionInfo['status'])) {
			bootbox.confirm("Do you really want to delete this session?", function (result) {
				if (result) {

					deleteSessionBtn.attr('disabled', null);

					d3.json(url, {
						method: 'DELETE',
						headers: {
							"Content-type": "application/json; charset=UTF-8"
						},
						body: JSON.stringify(function (response, error) {
							// We don't expect a response so ignoring it.

							if (error) {
								console.log(response)
								console.error(error)
								return
							}
						})
					});
				}
			});
		} else {
			// display an error
			bootbox.alert("Can't delete " + sessionId + "! It is still RUNNING.");
		}
	})
}

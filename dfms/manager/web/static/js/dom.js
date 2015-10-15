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

var SESSION_STATUS = ['Pristine', 'Building', 'Deploying', 'Running', 'Finished']
var STATUS_CLASSES = ['initialized', 'writing', 'completed', 'expired', 'deleted']
var TYPE_CLASSES   = ['app', 'container', 'socket', 'plain']
var TYPE_SHAPES    = {app:'rect', container:'parallelogram', socket:'parallelogram', plain:'parallelogram'}

var TO_MANY_LTR_RELS = ['consumers', 'streamingConsumers', 'outputs']
var TO_MANY_RTL_RELS = ['inputs', 'streamingInputs', 'producers']

function getRender() {

	var render = new dagreD3.render();

	// Add our custom shape (parallelogram, similar to the PIP PDR document)
	render.shapes().parallelogram = function(parent, bbox, node) {
		var w = bbox.width,
		h = bbox.height,
		points = [
		    { x: 0,     y: 0},
		    { x: w*0.8, y: 0},
		    { x: w,     y: -h},
		    { x: w*0.2, y: -h},
		];
		var shapeSvg = parent.insert("polygon", ":first-child")
		.attr("points", points.map(function(d) { return d.x + "," + d.y; }).join(" "))
		.attr("transform", "translate(" + (-w/2) + "," + (h/2) + ")");

		node.intersect = function(point) {
			return dagreD3.intersect.polygon(node, points, point);
		};

		return shapeSvg;
	};

	return render;
}

function loadSessions(serverUrl, tbodyEl, refreshBtn, scheduleNew, delay) {

	refreshBtn.attr('disabled');
	d3.json(serverUrl + '/api/sessions', function (error, response){
		if( error ) {
			console.error(error)
			refreshBtn.attr('disabled', null);
			return
		}

		var sessions = response;
		sessions.sort(function comp(a,b) {
			return (a.sessionId < b.sessionId) ? -1 : (a.sessionId > b.sessionId);
		});
		var rows = tbodyEl.selectAll('tr').data(sessions);
		rows.exit().transition().delay(0).duration(500).style('opacity',0.0).remove();
		rows.enter().append('tr').style('opacity', 0.0).transition().delay(0).duration(500).style('opacity',1.0);

		var idCells = rows.selectAll('td.id').data(function values(s) { return [s.sessionId]; });
		idCells.enter().append('td').classed('id', true).text(String)
		idCells.text(String)
		idCells.exit().remove()

		// See how many "thead tr th are there" in this table
		var nHeads = d3.select(tbodyEl.node().parentNode).selectAll('thead tr th')[0].length;
		if (nHeads > 1) {
			var statusCells = rows.selectAll('td.status').data(function values(s) { return [s.status]; });
			statusCells.enter().append('td').classed('status', true).text(function(s) { return SESSION_STATUS[s]; })
			statusCells.text(function(s) {return SESSION_STATUS[s]})
			statusCells.exit().remove()

			statusCells = rows.selectAll('td.details').data(function values(s) { return [s.sessionId]; });
			statusCells.enter().append('td').classed('details', true)
			    .append('a').attr('href', function(s) { return 'session?sessionId=' + s; })
			    .append('span').classed('glyphicon glyphicon-share-alt', true)
			statusCells.select('a').attr('href', function(s) { return 'session?sessionId=' + s; })
			statusCells.exit().remove()
		}

		refreshBtn.attr('disabled', null);

		if( scheduleNew ) {
			d3.timer(function(){
				loadSessions(serverUrl, tbodyEl, refreshBtn, true, delay);
				return true;
			}, delay);
		}
	});
}

function promptNewSession(serverUrl, tbodyEl, refreshBtn) {
	bootbox.prompt("Session ID", function(sessionId) {
		if( sessionId == null ) {
			return;
		}
		var xhr = d3.xhr(serverUrl + '/api/sessions');
		xhr.header("Content-Type", "application/json");
		xhr.post(JSON.stringify({sessionId: sessionId}), function(error, data) {
			if( error != null ) {
				console.error(error)
				bootbox.alert('An error occurred while creating session ' + sessionId + ': ' + error.responseText)
				return
			}
			loadSessions(serverUrl, tbodyEl, refreshBtn, false)
		});
	});
}

/**
 * Starts a regular background task that retrieves the current graph
 * specification from the REST server until the session's status is either
 * DEPLOYING or RUNNING, in which case no more graph structure changes are
 * expected.
 *
 * Using the graph specification received from the server the graph
 * g is updated adding nodes and edges as necessary.
 *
 * Once the status of the session is RUNNING or DEPLOYING, this task is not
 * scheduled anymore, and #startGraphStatusUpdates is called instead.
 *
 * @param g The graph
 * @param doSpecs A list of DOSpecs, which are simple dictionaries
 */
function startStatusQuery(g, serverUrl, sessionId, delay) {
	function updateGraph() {
		d3.json(serverUrl + '/api/sessions/' + sessionId, function(error, sessionInfo) {

			if (error) {
				console.error(error)
				return
			}

			var doSpecs = sessionInfo['graph']
			var status  = sessionInfo['sessionStatus']
			d3.select('#session-status').text(SESSION_STATUS[status])

			// Keep track of modifications to see if we need to re-draw
			var modified = false

			// #1: create missing nodes in the graph
			for(idx in doSpecs) {
				var doSpec = doSpecs[idx]
				modified |= _addNode(g, doSpec)
			}

			// #2: establish missing relationships
			for(idx in doSpecs) {
				var doSpec = doSpecs[idx]
				var lhOid = doSpec.oid

				// x-to-many relationships producing lh->rh edges
				for(relIdx in TO_MANY_LTR_RELS) {
					var rel = TO_MANY_LTR_RELS[relIdx]
					if( rel in doSpec ) {
						for(rhOid in doSpec[rel]) {
							modified |= _addEdge(g, lhOid, doSpec[rel][rhOid])
						}
					}
				}
				// x-to-many relationships producing rh->lh edges
				for(relIdx in TO_MANY_RTL_RELS) {
					var rel = TO_MANY_RTL_RELS[relIdx]
					if( rel in doSpec ) {
						for(rhOid in doSpec[rel]) {
							modified |= _addEdge(g, doSpec[rel][rhOid], lhOid)
						}
					}
				}
				// there currently are no x-to-one relationships producing rh->lh edges
				// there currently are no x-to-one relationships producing lh->rh edges
			}

			if( modified ) {
				drawGraph()
			}

			// During PRISITINE and BUILDING we need to update the graph structure
			// During DEPLOYING we call ourselves again anyway, because we need
			// to know when we go to RUNNING.
			// During RUNNING (or potentially FINISHED, if the execution is
			// extremely fast) we need to start updating the status of the graph
			if( status == 3 || status == 4 ) {
				startGraphStatusUpdates(serverUrl, sessionId, delay)
			}
			else if( status == 0 || status == 1 || status == 2 ){
				// schedule a new JSON request
				d3.timer(updateGraph, delay)
			}

		})
		// This makes d3.timer invoke us only once
		return true
	}
	d3.timer(updateGraph)
}

function _addNode(g, doSpec) {

	if( g.hasNode(g) ) {
		return false
	}

	var typeClass = doSpec.type
	var typeShape = TYPE_SHAPES[doSpec.type]
	var notes = ''
	if( doSpec.type == 'app' ) {
		var nameParts = doSpec.app.split('.')
		notes = nameParts[nameParts.length - 1]
	}
	else if( doSpec.type == 'plain' ) {
		notes = 'storage: ' + doSpec.storage
	}
	else if( doSpec.type == 'socket' ) {
		notes = 'port: ' + doSpec.port
	}

	var oid = doSpec.oid
	var html = '<div class="do-label" id="id_' + oid + '">';
	html += '<span>' + oid + '</span>';
	html += '<span class="notes">' + notes + '</span>'
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
	return true
}

function _addEdge(g, fromOid, toOid) {
	if( g.hasEdge(fromOid, toOid) ) {
		return false
	}
	if( !g.hasNode(fromOid) ) {
		console.error('No DataObject found with oid ' + fromOid)
		return false
	}
	if( !g.hasNode(toOid) ) {
		console.error('No DataObject found with oid ' + toOid)
		return false
	}
	g.setEdge(fromOid, toOid, {width: 40});
	return true
}


/**
 * Starts a regular background task that retrieves the current status of the
 * graph from the REST server, updating the current display to show the correct
 * colors
 */
function startGraphStatusUpdates(serverUrl, sessionId, delay) {
	function updateStates() {
		d3.json(serverUrl + '/api/sessions/' + sessionId + '/graph/status', function(error, response) {
			if (error) {
				console.error(error);
				return;
			}

			// Change from {A:1, B:2...} to [1,2...]
			var statuses = Object.keys(response).map(function(k) {return response[k]});

			// This works assuming that the status list comes in the same order
			// that the graph was created, which is true
			// Anyway, we could double-check in the future
			d3.selectAll('g.nodes').selectAll('g.node')
			.data(statuses).attr("class", function(s) { return "node " + STATUS_CLASSES[s]})

			var allCompleted = statuses.reduce(function(prevVal, curVal, idx, arr) {
				return prevVal && (curVal == 2);
			}, true);
			if (!allCompleted) {
				d3.timer(updateStates, delay);
			}
			else {
				// A final update on the session's status
				d3.json(serverUrl + '/api/sessions/' + sessionId + '/status', function(error, status) {
					if (error) {
						console.error(error);
						return;
					}
					d3.select('#session-status').text(SESSION_STATUS[status]);
				})
			}
		})
		return true;
	}
	d3.timer(updateStates);
}
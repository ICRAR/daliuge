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

var SESSION_STATUS = ['Pristine', 'Deploying', 'Running', 'Finished']
var STATUS_CLASSES = ['initialized', 'writing', 'completed', 'expired', 'deleted']
var TYPE_CLASSES   = ['app', 'container', 'socket', 'plain']
var TYPE_SHAPES    = {app:'rect', container:'parallelogram', socket:'parallelogram', plain:'parallelogram'}

var TO_MANY_LTR_RELS = ['consumers', 'streamingConsumers', 'outputs']
var TO_MANY_RTL_RELS = ['inputs', 'streamingInputs']
var  TO_ONE_RTL_RELS = ['producer']

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
		shapeSvg = parent.insert("polygon", ":first-child")
		.attr("points", points.map(function(d) { return d.x + "," + d.y; }).join(" "))
		.attr("transform", "translate(" + (-w/2) + "," + (h/2) + ")");

		node.intersect = function(point) {
			return dagreD3.intersect.polygon(node, points, point);
		};

		return shapeSvg;
	};

	return render;
}

/**
 * Starts a regular background task that loads the list of all sessions held in
 * the DataObjectManager
 *
 * @param serverUrl The serverl URL
 * @param delay The amount of time between calls
 */
function startLoadingSessions(ul, serverUrl, delay) {
	function loadSessions() {
		d3.json(serverUrl + '/api', function (error, response){
			if( error ) {
				console.error(error)
				return
			}

			sessions = response['sessions']
			var lis = ulEl.selectAll('li').data(sessions)

			// Updates
			lis.select('a')
				.attr('href', function(s) {return 'session?sessionId=' + s.sessionId})
				.text(function(s) {return s.sessionId + " (" + SESSION_STATUS[s.status] + ")"})

			// New items
			lis.enter().append('li').append('a')
				.attr('href', function(s) {return 'session?sessionId=' + s.sessionId})
				.text(function(s) {return s.sessionId + " (" + SESSION_STATUS[s.status] + ")"})

			// Old items
			lis.exit().remove()

			d3.timer(loadSessions, delay)
		})
		return true
	}
	d3.timer(loadSessions)
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
		d3.json(serverUrl + '/api/' + sessionId, function(error, sessionInfo) {

			if (error) {
				console.error(error)
				return
			}

			doSpecs = sessionInfo['graph']
			status  = sessionInfo['sessionStatus']
			d3.select('#session-status').text(SESSION_STATUS[status])

			// Keep track of modifications to see if we need to re-draw
			modified = false

			// #1: create missing nodes in the graph
			for(idx in doSpecs) {
				doSpec = doSpecs[idx]
				oid = doSpec.oid
				modified |= _addNode(g, doSpec)
			}

			// #2: establish missing relationships
			for(idx in doSpecs) {
				doSpec = doSpecs[idx]
				lhOid = doSpec.oid

				// x-to-many relationships producing lh->rh edges
				for(relIdx in TO_MANY_LTR_RELS) {
					rel = TO_MANY_LTR_RELS[relIdx]
					if( rel in doSpec ) {
						for(rhOid in doSpec[rel]) {
							modified |= _addEdge(g, lhOid, doSpec[rel][rhOid])
						}
					}
				}
				// x-to-many relationships producing rh->lh edges
				for(relIdx in TO_MANY_RTL_RELS) {
					rel = TO_MANY_RTL_RELS[relIdx]
					if( rel in doSpec ) {
						for(rhOid in doSpec[rel]) {
							modified |= _addEdge(g, doSpec[rel][rhOid], lhOid)
						}
					}
				}
				// x-to-one relationships producing rh->lh edges
				for(relIdx in TO_ONE_RTL_RELS) {
					rel = TO_ONE_RTL_RELS[relIdx]
					if( rel in doSpec ) {
						modified |= _addEdge(g, doSpec[rel], lhOid)
					}
				}
				// there currently are no x-to-one relationships producing lh->rh edges
			}

			if( modified ) {
				drawGraph()
			}

			// Only during PRISITINE we need to update the graph structure
			// Otherwise we need to start updating the status of the graph
			if( status != 0 ) {
				startGraphStatusUpdates(serverUrl, sessionId, delay)
			}
			else {
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
		nameParts = doSpec.app.split('.')
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
		d3.json(serverUrl + '/api/' + sessionId + '/graph/status', function(error, response) {
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
				d3.json(serverUrl + '/api/' + sessionId + '/status', function(error, status) {
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
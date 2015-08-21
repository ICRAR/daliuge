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

var TO_MANY_LTR_RELS = ['consumers', 'streamingConsumers', 'outputs']
var TO_MANY_RTL_RELS = ['inputs', 'streamingInputs']
var  TO_ONE_RTL_RELS = ['producer']

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
		d3.json(serverUrl + '/' + sessionId, function(error, sessionInfo) {

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
				modified = _addNode(g, doSpec)
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
							modified = _addEdge(g, lhOid, doSpec[rel][rhOid])
						}
					}
				}
				// x-to-many relationships producing rh->lh edges
				for(relIdx in TO_MANY_RTL_RELS) {
					rel = TO_MANY_RTL_RELS[relIdx]
					if( rel in doSpec ) {
						for(rhOid in doSpec[rel]) {
							modified = _addEdge(g, rhOid, doSpec[rel][lhOid])
						}
					}
				}
				// x-to-one relationships producing rh->lh edges
				for(relIdx in TO_ONE_RTL_RELS) {
					rel = TO_ONE_RTL_RELS[relIdx]
					if( rel in doSpec ) {
						modified = _addEdge(g, doSpec[rel], lhOid)
					}
				}
				// there currently are no x-to-one relationshipts producing lh->rh edges
			}

			if( modified ) {
				draw()
			}

			// DEPLOYING or RUNNING, we don't need to update the graph structure
			// anymore, but we need to start updating the status of the graph
			if( status == 1 || status == 2 ) {
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
	d3.timer(updateGraph, delay)
}

function _addNode(g, doSpec) {

	if( g.hasNode(g) ) {
		return false
	}

	var statusClass = STATUS_CLASSES[doSpec.status]
	var typeClass   = TYPE_CLASSES[doSpec.type]

	oid = doSpec.oid
	var html = '<div id="' + oid + '">';
	html += '<span class="status ' + statusClass + '"></span>';
	html += '<span class="name">' + oid + '</span>';
	html += "</div>";
	g.setNode(oid, {
		labelType: "html",
		label: html,
		rx: 5,
		ry: 5,
		padding: 0,
		class: typeClass
	});
	return true
}

function _addEdge(g, fromOid, toOid) {
	if( g.hasEdge(fromOid, toOid) ) {
		return false
	}
	g.setEdge(fromOid, toOid, { label: "dependency", width: 40 });
	return true
}


/**
 * Starts a regular background task that retrieves the current status of the
 * graph from the REST server, updating the current display to show the correct
 * colors
 */
function startGraphStatusUpdates(serverUrl, sessionId, delay) {
	function updateStates() {
		d3.json(serverUrl + '/' + sessionId + '/graph/status', function(error, response) {
			if (error) {
				console.error(error)
				return
			}
			allCompleted = true
			for (var id in response) {
				var status = response[id]
				var statusClass = STATUS_CLASSES[status]
				if ( status != 2 ) { // 2 == completed
					allCompleted = false
				}
				inner.select('#' + id + ' span.status')
				.attr('class', 'status ' + statusClass)
			}
			if (!allCompleted) {
				d3.timer(updateStates, delay)
			}
			else {
				d3.json(serverUrl + '/' + sessionId + '/status', function(error, status) {
					if (error) {
						console.error(error)
						return
					}
					d3.select('#session-status').text(SESSION_STATUS[status])
				})
			}
		})
		return true
	}
	d3.timer(updateStates, delay)
}
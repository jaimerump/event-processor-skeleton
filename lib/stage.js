/**
 * Pipelines load all of the data they'll need, then run their array of stages.
 *
 * @author Jaime Rump
 */

let _ 		= require('lodash');
let async	= require('async');
let debug = require('debug')('event-processor-skeleton:stage');

class Stage {

	/**
	 * Constructor
	 * @param Object app
	 * @param  Function callback   
	 */
	constructor( app, callback ) {

		this.app = app;
		this.callback = callback;

	}

	/**
	 * Runs the stage against the given data.
	 * 
	 * @param  Object	data 			The relevant data
	 * @param  Object	event 		The event payload from the queue
	 * 
	 */
	run( data, event ) {

		return this.callbackNormally();

	}

	/**
	 * Calls back normally
	 */
	callbackNormally( err ) {
		return this.callback( err, false, null );
	}

	/**
	 * Terminates the parent pipeline early
	 */
	terminateEarly() {
		return this.callback( null, true, null );
	}

	/**
	 * Calls back with new data
	 */
	callbackWithData( new_data ) {
		return this.callback( null, false, new_data );
	}

}

// Set exports
module.exports = Stage;
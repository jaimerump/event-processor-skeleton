/**
 * Pipelines load all of the data they'll need, then run their array of stages.
 *
 * @author Jaime Rump
 */

let _ 		= require('lodash');
let async	= require('async');
let debug = require('debug')('event-processor-skeleton:pipeline');

const TERMINATE_EARLY = "Terminating early";

class Pipeline {

	/**
	 * Constructor
	 * @param Object app The application object with all dependencies attached
	 */
	constructor( app ) {

		this.app = app;

	}

	/**
	 * Loads the data for the given payload
	 * 
	 * @param  Object   event    	The event payload from the queue
	 * @param  Function callback  
	 */
	loadData( event, callback ) {

		return callback( null, {} );

	}

	/**
	 * Runs the stages against the given data.
	 * 
	 * @param  Object   event    	The event payload from the queue
	 * @param  Function callback  
	 */
	run( event, callback ) {

		let s = this;

		s.loadData( event, function( err, data ) {

			if( err ) return callback(err);

			async.eachSeries( s.stages, function( stage, each_callback ) {

				stage.new( app, function(err, halt, new_data) {
					
					// Allow stages to terminate early
					if( halt ) return each_callback( TERMINATE_EARLY );

					// Allow stage to overwrite data
					if( !_.isEmpty(new_data) ) data = new_data;

					return each_callback(err);

				}).run( data, event);

			}, function(err) {
				// Don't propagate early halts
				if( err == TERMINATE_EARLY ) return callback();

				return callback(err);
			}); // async.eachSeries

		}); // loadData

	}

	/**
	 * Names all of the stages
	 */
	nameStages() {
		return _.map( this.stages, function(stage_class) {
			return stage_class.new().name;
		}); // _.map
	}

	/**
	 * Names the requirements of all of the stages
	 */
	nameRequirements() {
		return _.map( this.stages, function(stage_class) {
			let stage = stage_class.new();
			return {
				name: stage.name,
				requirements: stage.requirements()
			};
		}); // _.map
	}

}

// Set exports
module.exports = Pipeline;
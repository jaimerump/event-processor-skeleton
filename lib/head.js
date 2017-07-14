/**
 * The head initializes the app and dependencies, sets up queue connections, and 
 * passes incoming events off to the relevant Pipeline.
 *
 * @summary Main entry point for queue processors.
 * 
 * @author Jaime Rump
*/

let debug = require('debug')('event-processor-skeleton:head');
let async = require('async');
let _     = require('lodash');

// Internal libs
let QueueWrapper = require('queue-wrapper');

class Head {

  /**
   * Constructor
   * @param String name             The type of the processor, queue-name
   */
  constructor(name){
    
    this.name = name;
    this.app = {
      pipelines: {
      }
    };

  }

  /**
   * Sets up dependencies
   * @param  Function callback In case async setup is necessary
   */
  setupDependencies(callback) {
    callback();
  }

  /**
   * Initializes the queue connections
   * @param  Function callback In case async setup is necessary
   */
  initQueues(callback) {

    let s = this;

    // Set up main queue and subqueue
    let amqp_options = {
      host: process.env.MQ_URL || 'amqp://localhost',
      queue: 'main',
      delayed: true
    }
    let subqueue_options = {
      host: process.env.MQ_URL || 'amqp://localhost',
      queue: `queue-${s.name}`
    }

    s.app.main_queue = new QueueWrapper( amqp_options );
    s.app.subqueue   = new QueueWrapper( subqueue_options );

    // Connect to the Queue
    s.app.main_queue.connect( function( err, conn ) {

      if(err) {
        debug("ERROR Connecting");
        return callback(err);
      }

      debug("Connected to main Message Queue");

      s.app.subqueue.connect( function( err, conn ) {

        if(err) {
          debug("ERROR Connecting");
          return callback(err);
        }

        debug("Connected to "+s.name+" Message Queue");

        return callback(err);

      }); // app.subqueue.connect

    }); // app.main_queue.connect

  }

  /**
   * Handles each incoming event
   * @param  Object err     
   * @param  Object message 
   */
  eventProcessor( err, message ) {

    let s = this;

    // Pull out the event for pipeline pathing
    let event = message.event;

    // Check Pipelines on app
    if( !_.isUndefined( s.app.pipelines[event] ) ) {
      debug("Piping to " + event + " pipeline");

      s.app.pipelines[event].new( s.app ).run( message, function( err, data ) {
          
        debug("Done with message");
        if( err ) {
          s.error( { message: err }, message, function(){
            debug("Sent error notification");
          });
        } 

      });

    } else {
      s.missingPipeline( message, function(err) {
        debug(`Missing pipeline for ${message.domain}:${message.event}`);
      });
    }

  }

  /**
   * Logs a missing pipeline event
   * @param  Object   event    
   * @param  Function callback 
   */
  missingPipeline( event, callback ) {

    let user_id = ( event ) ? event.user_id : null;

    let payload = {
      domain: "log",
      event: "missing-pipeline",
      user_id: user_id,
      payload: event
    };
    this.app.main_queue.send( payload, callback );

  }

  /**
   * Handles an error
   * @param  Object   err      
   * @param  Object   event    
   * @param  Function callback 
   */
  error( err, event, callback ) {

    debug("Error occurred:", err, event)

    err.repository = 'queue-'+this.name;
    err.environment = ( process.env.STAGING ) ? 'staging' : 'production';

    let user_id = ( event ) ? event.user_id : null;

    let error_payload = {
      domain: "notification",
      event: "error",
      user_id: user_id,
      payload: {
        event_payload: event,
        err: err
      }
    };

    this.app.main_queue.send( error_payload, callback );

  }

  /**
   * Boots everything up and starts it running
   */
  run() {

    let s = this;

    s.setupDependencies(function(err) {

      if(err) {
        s.error({ message: err }, {}, function() {
          process.exit();
        });
      }

      s.initQueues(function(err) {

        if(err) {
          s.error({ message: err }, {}, function() {
            process.exit();
          });
        }

        if( s.name == 'sorter' ) {

          // Watch the main queue
          s.app.main_queue.watch( function(err, message) {
            s.cached_message = message; // Cache message for exception handler to use
            s.eventProcessor(err, message)
          });

        } else {

          // Watch the subqueue
          s.app.subqueue.watch( function(err, message) {
            s.cached_message = message; // Cache message for exception handler to use
            s.eventProcessor(err, message)
          });

        } // if sorter

      }); // initQueues

    }); // setupDependencies

  }

}

// Set exports
module.exports = Head;
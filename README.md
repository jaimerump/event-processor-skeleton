# event-processor-skeleton
A framework upon which to build other event processors.

Each event is sorted into its own Pipeline, which receives the event from the queue, loads any necessary data, then runs the data through a series of Stages to perform various business tasks. Pipelines also have several utility methods. They can print their own name, as well as the names of all of their Stages, and the data that each Stage requires.

#!env /usr/local/bin/node

EventEmitter = require('events').EventEmitter
RecordProcessor = require('../recordprocessor')
logger = require('../logger')

class WordProcessor
	processRecord : (data, seq, key, cb) ->
		logger.debug("WordProcessor : processing [#{data.length}] records")
		###
			Your Processing Logic
		###
		do cb
		return
	initialize : (shardId, cb) ->
		###
			Your Initialization Logic
		###
		do cb 
		return
	shutdown : (reason, cb) ->
		###
			Your Shutdown logic
		###
		do cb
		return

wordProcessor = new WordProcessor
recordProcessor = new RecordProcessor(wordProcessor)
do recordProcessor.run
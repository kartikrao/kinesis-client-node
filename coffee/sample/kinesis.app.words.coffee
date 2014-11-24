#!env /usr/local/bin/node
settings = require './kinesis.app.words.settings'
EventEmitter = require('events').EventEmitter
RecordProcessor = require('../recordprocessor')
logger = require('../logger')

class WordProcessor
	processRecord : (data, seq, key, cb) ->
		logger.info("WordProcessor : processing [#{data.length}] records")
		###
		Your Processing Logic
		###
		process.nextTick -> cb null
		return

wordProcessor = new WordProcessor
recordProcessor = new RecordProcessor(wordProcessor)
do recordProcessor.run
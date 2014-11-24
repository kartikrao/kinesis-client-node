winston = require 'winston'
env = process.env.NODE_ENV
path = require 'path'

logfile = path.resolve "#{__dirname}/../logs/worker.log"
level = if env is 'production' then 'info' else 'debug'

Loggers = 
	transports :
		file:
			level  : level
			filename: logfile
			maxsize : 67108864
			maxFiles: 8
			json: false

winston.add(winston.transports.File, Loggers.transports.file);
winston.remove(winston.transports.Console);
winston.addColors({'debug' : 'green', 'info' : 'blue', 'error' : 'red'});
module.exports = winston
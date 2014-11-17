#!env node

path = require 'path'
optimist = require('optimist')
_ = require 'underscore'

class KCLHelper
	constructor : ->
		@kclpath = __dirname
		@separator = ":"
	getKclDir : -> @kclpath
	getKclJarPath : (cb) ->
		self = @
		glob "#{@kclpath}/jars/**/*jar", (err, files=[]) ->
			cb files.join(self.separator)
			return
		return
	getKclClasspath : (propertyPath=null, paths, cb) ->
		rpaths = []
		rpaths.push path.resolve(path) for path in paths
		if propertyPath?
			rpaths.push path.resolve(propertyPath)
		self = @
		@getKclJarPath (jarpath=[]) ->
			classpath = Array.concat rpaths, jarpath
			cb classpath.join(self.separator)
			return
		return
	getKclAppCommand : (java, mld_class, properties, paths=[]) ->
		baseName = properties
		if properties.indexOf("/") > -1
			baseName = properties.substring(properties.lastIndexOf("/") + 1)
		"#{java} -cp #{@getKclClasspath(properties, paths)} #{mld_class} #{baseName}"

kclhelper = new KCLHelper

argv = optimist.boolean ["print_classpath", "print_command"]

args = _.clone argv

if args.sample?
	if args.properties?
		console.error "Replacing provided properties with sample properties due to arg --sample"
	args.properties = if args.sample.indexOf("/") > -1 then args.sample else "#{__dirname}/#{args.sample}"

if args.print_classpath is "true"
	kclhelper.getKclClasspath (cp) ->
		console.log cp
		return
else if args.print_command?
	if args.java? and args.properties?
		mld_class = "com.amazonaws.services.kinesis.multilang.MultiLangDaemon"
		paths = if args.paths? then args.paths.split(",") else []
		kclhelper.getKclAppCommand args.java, mld_class, args.properties, paths
	else
		console.error "Must provide arguments --java and --properties\n"
else
	usage = """
		--java  $path_to_java_executable - Required
		--props $rel_path_to_properties  - Required
		--print_classpath [false]        - Show classpath
		--print_command   [false]        - Show Java command
		--sample          [false]        - Use sample properties
	"""
	console.error usage




gulp    = require 'gulp'
gutil   = require 'gulp-util'
clean   = require 'gulp-clean'
header  = require 'gulp-header'
coffee  = require 'gulp-coffee'
chmod   = require 'gulp-chmod'
path    = require 'path'
Download= require 'download'
progress= require 'download-status'
fs = require 'fs-extra'
async = require 'async'

pkg    = require './package.json'
banner = [ '#!/usr/bin/env node'
	'/**', 
	' * <%= pkg.description %>',
	' * @version   : <%= pkg.version %>',
	' * @copyright : <%= pkg.copyright %>',
	' * @license   : <%= pkg.license %>',
	' **/',
	''
].join('\n')

JARS = [
	{groupId: 'com.amazonaws', artifactId: 'amazon-kinesis-client', version: '1.2.0'},
	{groupId: 'com.fasterxml.jackson.core', artifactId: 'jackson-core', version: '2.1.1'},
	{groupId: 'org.apache.httpcomponents', artifactId: 'httpclient', version: '4.2'},
	{groupId: 'org.apache.httpcomponents', artifactId: 'httpcore', version: '4.2'},
	{groupId: 'com.fasterxml.jackson.core', artifactId: 'jackson-annotations', version: '2.1.1'},
	{groupId: 'commons-codec', artifactId: 'commons-codec', version: '1.3'},
	{groupId: 'joda-time', artifactId: 'joda-time', version: '2.4'},
	{groupId: 'com.amazonaws', artifactId: 'aws-java-sdk', version: '1.7.13'},
	{groupId: 'com.fasterxml.jackson.core', artifactId: 'jackson-databind', version: '2.1.1'},
	{groupId: 'commons-logging', artifactId: 'commons-logging', version: '1.1.1'}
]

JAR_PATH   = path.resolve "#{__dirname}/jars"
MAVEN_REPO = "http://search.maven.org/remotecontent?filepath="
LIB_JS     = "./lib"
SAMPLE_JS  = "./lib/sample"

gulp.task 'setup', (taskCallback) ->
	getJarPaths = (meta) ->
		{artifactId, version, groupId} = meta
		path = groupId.replace(/\./g, '/')
		name = "#{artifactId}-#{version}.jar"
		{remote : "#{MAVEN_REPO}#{path}/#{artifactId}/#{version}/#{name}", local : "#{JAR_PATH}/#{name}", name : name}

	downloadJar = (src, dest) ->

	# Ensure JAR directory exists
	fs.ensureDir JAR_PATH, (err) ->
		if err?
			console.log err
			return -1

		# Loop through artifacts
		agents = {}
		for jar in JARS
			{name, remote, local} = getJarPaths(jar)
			agents[name] = do ->
				r = remote
				l = local
				n = name
				(cb) ->
					fs.open local, 'r', (err) ->
						if err?
							download = new Download({extract: false})
							.get(r)
							.dest(l)
							.use(progress())
							download.run (derr) -> cb derr, n
						else
							cb null, n
						return
		async.series agents, (serr, results) ->
			if serr?
				console.log "Error downloading JARS", serr
			taskCallback serr
			return
		return
	return

gulp.task 'cleanlib', ->
	gulp.src('./lib/*.js', {read: false})
	.pipe(clean())

gulp.task 'cleansample', ->
	gulp.src('./lib/sample/*', {read: false})
	.pipe(clean())

gulp.task 'sample', ['cleansample'], ->
	gulp.src('./src/sample/*.coffee')
	.pipe(coffee())
	.pipe(header(banner, {pkg: pkg}))
	.pipe(chmod({owner: {execute: true, write: true, read: true}}))
	.pipe(gulp.dest(SAMPLE_JS))
	.on('error', gutil.log)
	gulp.src(['./src/sample/*.properties', './src/sample/*.js'])
	.pipe(gulp.dest(SAMPLE_JS))
	.on('error', gutil.log)

gulp.task 'lib', ['cleanlib'], ->
	gulp.src('./src/*.coffee')
	.pipe(coffee())
	.pipe(header(banner, {pkg: pkg}))
	.pipe(chmod({owner: {execute: true, write: true, read: true}}))
	.pipe(gulp.dest(LIB_JS))
	.on('error', gutil.log)

gulp.task 'watch', ->
	gulp.watch './src/*.coffee', ['lib']
	gulp.watch ['./src/sample/*.coffee', './src/sample/*.js', './src/sample/*.properties'], ['sample']

gulp.task 'sample-command', ['setup'], ->
	gutil.log "./lib/kclhelper.js --print_command --java /usr/bin/java --props ./lib/sample/ml-daemon.properties"

gulp.task 'default', ['lib', 'sample', 'watch']
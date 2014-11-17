gulp    = require 'gulp'
gutil   = require 'gulp-util'
clean   = require 'gulp-clean'
header  = require 'gulp-header'
coffee  = require 'gulp-coffee'
path    = require 'path'
Download= require 'download'
progress= require 'download-status'
fs = require 'fs-extra'
async = require 'async'

pkg    = require './package.json'
banner = ['/**', 
	' * <%= pkg.description %>',
	' * @version   : <%= pkg.version %>',
	' * @copyright : <%= pkg.copyright %>',
	' * @license   : <%= pkg.license %>',
	' **/',
	''
].join('\n')

JARS = [{groupId: 'com.amazonaws', artifactId: 'amazon-kinesis-client', version: '1.2.0'},
{groupId: 'com.fasterxml.jackson.core', artifactId: 'jackson-core', version: '2.1.1'},
{groupId: 'org.apache.httpcomponents', artifactId: 'httpclient', version: '4.2'},
{groupId: 'org.apache.httpcomponents', artifactId: 'httpcore', version: '4.2'},
{groupId: 'com.fasterxml.jackson.core', artifactId: 'jackson-annotations', version: '2.1.1'},
{groupId: 'commons-codec', artifactId: 'commons-codec', version: '1.3'},
{groupId: 'joda-time', artifactId: 'joda-time', version: '2.4'},
{groupId: 'com.amazonaws', artifactId: 'aws-java-sdk', version: '1.7.13'},
{groupId: 'com.fasterxml.jackson.core', artifactId: 'jackson-databind', version: '2.1.1'},
{groupId: 'commons-logging', artifactId: 'commons-logging', version: '1.1.1'}]

JAR_PATH   = path.resolve "#{__dirname}/jars"
MAVEN_REPO = "http://search.maven.org/remotecontent?filepath="
LIB_JS     = "./lib"

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

gulp.task 'clean', ->
	gulp.src('./lib/*.js', {read: false})
	.pipe(clean())

gulp.task 'lib', ['clean'], ->
	gulp.src('./coffee/*.coffee')
	.pipe(coffee())
	.pipe(header(banner, {pkg: pkg}))
	.pipe(gulp.dest(LIB_JS))
	.on('error', gutil.log)

gulp.task 'default', ['lib']

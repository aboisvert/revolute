require 'buildr/scala'

VERSION_NUMBER = "0.0.1"

repositories.remote << "http://www.ibiblio.org/maven2/"
repositories.remote << 'http://repo1.maven.org/'

HADOOP_HOME = "/opt/boisvert/hadoop-0.20.2"

CASCADING_DISTRO = "org.cascading:cascading:tgz:wip-2.0"
download(artifact(CASCADING_DISTRO) => 'http://files.concurrentinc.com/cascading/2.0/cascading-2.0.0-wip-200-hadoop-0.20.2%2B.tgz')

CASCADING_TMP = File.join(ENV['TMP'] || '/tmp', 'cascading-wip-2.0')
unzip(CASCADING_TMP => artifact(CASCADING_DISTRO))
file(CASCADING_TMP).invoke
FileList[CASCADING_TMP + '/cascading-2.0.0-wip-200-hadoop-0.20.2+/*'].each do |f|
  FileUtils.mv(f, CASCADING_TMP)
end

CASCADING_CORE = \
  FileList[CASCADING_TMP + "/cascading-core-*.jar"] + \
  FileList[CASCADING_TMP + "/cascading-hadoop-*.jar"] + \
  FileList[CASCADING_TMP + "/cascading-local-*.jar"] + \
  FileList[CASCADING_TMP + "/lib/**/*.jar"]

HADOOP = \
  FileList[HADOOP_HOME + "/hadoop-*-core.jar"] + \
  FileList[HADOOP_HOME + "/lib/*.jar"]

desc 'Scala-based query language for Hadoop'
define "revolute" do
  project.version = VERSION_NUMBER
  project.group = "revolute"

  compile.with CASCADING_CORE, HADOOP
  compile.using :deprecation => true,
                :other => ['-unchecked', '-Xprint-types']

  test.using :scalatest
  test.using :properties => { 'hadoop.home' => HADOOP_HOME }
  test.with

  doc.using :scaladoc

  package :jar
  package :scaladoc
end


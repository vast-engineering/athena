import java.lang.{Process => JProcess, ProcessBuilder => JProcessBuilder}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import sbt._

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

object CassandraUtils {


  object Keys {
    val downloadHome = settingKey[String]("The directory where cassandra distributions will be stored.")
    val cassandraVersion = settingKey[String]("The version of cassandra to be used.")
    val instanceName = settingKey[String]("The name of the cassandra instance.")
    val host = settingKey[String]("The hostname for the cassandra instance.")
    val cqlProtocolPort = settingKey[Int]("The port for the CQL native cassandra protocol.")
    val cleanInstance = settingKey[Boolean]("If true, the cassandra instance will be cleaned before start.")
    val recreateConfiguration = settingKey[Boolean]("If true, the configuration will be refreshed.")
    val initialSchemaFile = settingKey[Option[String]]("The name of file, located in the test resources directory, used to intialize the database after starting.")

    val cassandraInstanceHome = taskKey[File]("The location of the cassandra instance.")
    val cassandraDistributionsHome = taskKey[File]("The directory where downloaded instances of cassandra will be stored.")
    val startCassandra = taskKey[Process]("Start the external instance of cassandra.")

  }

  private val ExecutableMask = Integer.parseInt("0111", 8)

  private val ExcludedConfFiles = Seq("cassandra.yaml", "cassandra-env.sh", "log4j-server.properties")

  val cassandraTestSettings = Seq(
    Keys.downloadHome := {
      val homeDir = file(System.getProperty("user.home"))
      if(!homeDir.exists()) {
        throw new RuntimeException("Cannot locate home directory.")
      }
      (homeDir / ".cassandra-distrib").getAbsolutePath
    },
    Keys.cassandraVersion := "2.0.9",
    Keys.instanceName := s"test-cassandra-${Keys.cassandraVersion.value}",
    Keys.host := "localhost",
    Keys.cqlProtocolPort := 9142,
    Keys.cleanInstance := true,
    Keys.recreateConfiguration := false,
    Keys.initialSchemaFile := Some("schema.cql"),

    Keys.cassandraInstanceHome := {
      val parentDir = sbt.Keys.target.value / "test-cassandra"
      IO.createDirectory(parentDir)
      parentDir
    },
    Keys.cassandraDistributionsHome := {
      val dir = file(Keys.downloadHome.value)
      IO.createDirectory(dir)
      dir
    },

    Keys.startCassandra := {
      val schemaFile = Keys.initialSchemaFile.value map { s => (sbt.Keys.resourceDirectory in Test).value / s }
      startCassandra(Keys.cassandraInstanceHome.value, Keys.cassandraDistributionsHome.value,
        Keys.instanceName.value, Keys.cassandraVersion.value, Keys.host.value,
        Keys.cqlProtocolPort.value, Keys.cleanInstance.value, Keys.recreateConfiguration.value, schemaFile, sbt.Keys.streams.value.log)
    },

    (sbt.Keys.executeTests in Test) <<=
      (sbt.Keys.executeTests in Test, Keys.startCassandra, sbt.Keys.streams) { (et, sc, str) =>
        sc.flatMap { process =>
          str.flatMap { s =>
            et andFinally {
              s.log.info("Shutting down external Cassandra instance.")
              process.destroy()
              process.exitValue()
              s.log.info("Cassandra shut down.")
            }
          }
        }
      }

  )


  private def startCassandra(base: File, downloadHome: File, instanceName: String, version: String, host: String, rpcPort: Int, clean: Boolean, recreateConf: Boolean, initialSchema: Option[File], log: Logger): Process = {

    log.info(s"Starting external cassandra demon at $host:$rpcPort")
    val instanceBase = base / instanceName
    val distributionHome = resolveCassandraHome(downloadHome, version)
    val instanceConf = instanceBase / "conf"

    if(clean) {
      IO.delete(instanceBase)
    }

    if(clean || recreateConf) {

      IO.createDirectory(instanceBase)
      IO.createDirectory(instanceConf)

      val logDir = instanceBase / "log"
      IO.createDirectory(logDir)
      val dataDir = instanceBase / "data"
      IO.createDirectory(dataDir)

      val distributionConf = distributionHome / "conf"

      val confFiles = PathFinder(distributionConf) ** "*" filter {
        !ExcludedConfFiles.contains(_)
      }
      val confSources = confFiles pair Path.rebase(distributionConf, instanceConf)
      IO.copy(confSources, overwrite = true)

      val distributionEnv = distributionConf / "cassandra-env.sh"
      val instanceEnv = instanceConf / "cassandra-env.sh"

      //TODO: Add customization of the env file
      IO.copyFile(distributionEnv, instanceEnv)

      //customize the logger
      val loggerLines = IO.readLines(distributionConf / "log4j-server.properties")
      val loggerReplacements = Map(
        "log4j.appender.R.File=/var/log/cassandra/system.log" -> s"log4j.appender.R.File=${instanceBase.getAbsolutePath}/log/system.log"
      )
      IO.writeLines(instanceConf / "log4j-server.properties", replaceLines(loggerLines, loggerReplacements))

      val distributionYaml = IO.readLines(distributionConf / "cassandra.yaml")
      val yamlReplacements = Map(
        "rpc_address: localhost" -> s"rpc_address: $host",
        "    - /var/lib/cassandra/data" -> s"    - ${instanceBase.getAbsolutePath}/data/data",
        "listen_address: localhost" -> s"listen_address: $host",
        "commitlog_directory: /var/lib/cassandra/commitlog" -> s"commitlog_directory: ${instanceBase.getAbsolutePath}/data/commitlog",
        "saved_caches_directory: /var/lib/cassandra/saved_caches" -> s"saved_caches_directory: ${instanceBase.getAbsolutePath}/data/saved_caches",
        "native_transport_port: 9042" -> s"native_transport_port: $rpcPort"
      )

      IO.writeLines(instanceConf / "cassandra.yaml", replaceLines(distributionYaml, yamlReplacements))
    }


    val started = new CountDownLatch(1)
    val logger = new ProcessLogger {
      override def error(s: => String): Unit = {
//        log.error(s)
      }

      override def buffer[T](f: => T): T = {
        f
      }

      override def info(s: => String) {
        val line = s
        //log.info(s)
        if(line.contains("Listening for thrift clients")) {
          started.countDown()
        }
      }
    }

    val cassandraProcess = {
      val startScript = (distributionHome / "bin" / "cassandra").getAbsolutePath
      val startCommand = Seq(startScript, "-f")
      val builder = new JProcessBuilder(startCommand)
      val environment = builder.environment
      environment.put("CASSANDRA_CONF", instanceConf.getAbsolutePath)
      //TODO: Allow a custom JAVA_HOME
      environment.put("JAVA_HOME", System.getProperty("java.home"))
      Process(builder).run(logger)
    }

    try {
      if(!started.await(60, TimeUnit.SECONDS)) {
        throw new RuntimeException("Timed out waiting for cassandra to start.")
      }
      log.info("External cassandra demon started.")
      initialSchema.foreach { schemaFile =>
        log.info(s"Initializing instance with file ${schemaFile.getAbsolutePath}")
        val cqlsh = (distributionHome / "bin" / "cqlsh").getAbsolutePath
        val startCommand = Seq(cqlsh, "-f", schemaFile.getAbsolutePath)
        val cqlBuilder = new JProcessBuilder(startCommand)
        val cqlEnv = cqlBuilder.environment
        cqlEnv.put("CASSANDRA_CONF", instanceConf.getAbsolutePath)
        //TODO: Allow a custom JAVA_HOME
        cqlEnv.put("JAVA_HOME", System.getProperty("java.home"))

        Process(cqlBuilder) !! logger
      }
    } catch {
      case NonFatal(e) =>
        cassandraProcess.destroy()
        throw e
    }

    log.info("Cassandra initialization complete.")

    cassandraProcess

  }

  private def resolveCassandraHome(distributionHome: File, version: String): File = {

    val distArchiveName = s"apache-cassandra-$version-bin.tar.gz"
    val distArchive = distributionHome / distArchiveName
    if(!distArchive.exists()) {
      download(version, distributionHome)
    }

    distributionHome / s"apache-cassandra-$version"

  }

  private def download(version: String, destinationRoot: File): File =  {
    val archiveName = s"apache-cassandra-$version-bin.tar.gz"
    val url = new URL("http://archive.apache.org/dist/cassandra/" + version + "/" + archiveName)
    val dest = destinationRoot / archiveName
    IO.download(url, dest)
    IO.gzipFileIn(dest) { is =>
      val tar = new TarArchiveInputStream(is)
      var entry = tar.getNextEntry
      while(entry != null) {
        val destFile = destinationRoot / entry.getName
        if(entry.isDirectory) {
          IO.createDirectory(destFile)
        } else {
          IO.transfer(tar, destFile)
          if((tar.getCurrentEntry.getMode & ExecutableMask) != 0) {
            destFile.setExecutable(true)
          } else {
            destFile.setExecutable(false)
          }
        }
        entry = tar.getNextEntry
      }
      tar.close()
    }

    dest
  }

  private def replaceLines(lines: Seq[String], replacements: Map[String, String], expectExhaustive: Boolean = true) = {

    var replaced = 0
    val result = lines.collect {
      case line if replacements.contains(line) =>
        replaced = replaced + 1
        replacements(line)
      case line =>
        line
    }
    if(expectExhaustive && replaced != replacements.size) {
      throw new RuntimeException(s"looking to make ${replacements.size} matches replacement but made $replaced. It's likely that the build does not understand this version ")
    }
    result

  }


}


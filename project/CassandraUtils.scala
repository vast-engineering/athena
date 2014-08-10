import java.lang.{ProcessBuilder => JProcessBuilder}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.datastax.driver.core.Cluster
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import sbt._
import sbt.Keys._

import scala.collection.JavaConversions._
import com.vast.sbtlogger.SbtLogger

import scala.util.control.NonFatal

object CassandraUtils {

  object Keys {
    val host = settingKey[String]("The hostname for the cassandra instance.")
    val cqlProtocolPort = settingKey[Int]("The port for the CQL native cassandra protocol.")
    val createSchema = settingKey[Boolean]("If true, the test framework will create the necessary schema.")
    val initialSchemaFile = settingKey[Option[String]]("The name of file, located in the test resources directory, used to initialize the database after starting.")

    //keys relating to the auto-provisioned instance
    val provisionServer = settingKey[Boolean]("If true, an instance of Cassandra will be automatically provisioned by the test suite.")
    val downloadHome = settingKey[String]("The directory where cassandra distributions will be stored.")
    val cassandraVersion = settingKey[String]("The version of cassandra to be used.")
    val instanceName = settingKey[String]("The name of the cassandra instance.")
    val cleanInstance = settingKey[Boolean]("If true, the cassandra instance will be cleaned before start.")
    val recreateConfiguration = settingKey[Boolean]("If true, the configuration will be refreshed.")

    val cassandraInstanceHome = taskKey[File]("The location of the cassandra instance.")
    val cassandraDistributionsHome = taskKey[File]("The directory where downloaded instances of cassandra will be stored.")
  }

  //I *HATE* that this has to be a var, but there's no good way for the test hooks
  // to modify build state such that I can store this so it can be killed
  private var cassandraProcess: Option[Process] = None

  private val ExecutableMask = Integer.parseInt("0111", 8)

  private val ExcludedConfFiles = Seq("cassandra.yaml", "cassandra-env.sh", "log4j-server.properties")

  val cassandraTestSettings = Seq(
    Keys.provisionServer := true,
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
    Keys.createSchema := true,
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

    testOptions in Test += Tests.Setup(
      () => {
        try {
          val log = sbt.Keys.streams.value.log
          if (Keys.provisionServer.value) {
            cassandraProcess = Some(provisionInstance(Keys.cassandraInstanceHome.value, Keys.cassandraDistributionsHome.value,
              Keys.instanceName.value, Keys.cassandraVersion.value, Keys.host.value,
              Keys.cqlProtocolPort.value, Keys.cleanInstance.value, Keys.recreateConfiguration.value,
              log))
          }
          if (Keys.createSchema.value) {
            (Keys.initialSchemaFile.value map { s => (sbt.Keys.resourceDirectory in Test).value / s}).foreach { schemaFile =>
              setupInstance(Keys.host.value, Keys.cqlProtocolPort.value, schemaFile, log)
            }
          }
        } catch {
          case NonFatal(e) if Keys.provisionServer.value =>
            shutdownInstance(sbt.Keys.streams.value.log)
            throw e
        }
      }),

    testOptions in Test += Tests.Cleanup(
      () => {
        if(Keys.provisionServer.value) {
          val log = sbt.Keys.streams.value.log
          shutdownInstance(log)
        }
      }
    )

  )

  private def shutdownInstance(log: Logger): Unit = {
    log.info("Tearing down cassandra instance.")
    cassandraProcess match {
      case Some(p) => p.destroy()
      case None => log.error("Could not find cassandra process to kill! You may have an orphan process.")
    }
  }

  private def provisionInstance(base: File, downloadHome: File, instanceName: String,
                                version: String, host: String, rpcPort: Int, clean: Boolean,
                                recreateConf: Boolean, log: Logger): Process = {

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

    val startScript = (distributionHome / "bin" / "cassandra").getAbsolutePath
    val startCommand = Seq(startScript, "-f")
    val builder = new JProcessBuilder(startCommand)
    val environment = builder.environment
    environment.put("CASSANDRA_CONF", instanceConf.getAbsolutePath)
    //TODO: Allow a custom JAVA_HOME
    environment.put("JAVA_HOME", System.getProperty("java.home"))

    val cassandraProcess = Process(builder).run(logger)
    started.await(30, TimeUnit.SECONDS)
    log.info("Cassandra initialization complete.")

    cassandraProcess

  }

  private def setupInstance(host: String, port: Int, schemaFile: File, log: Logger) = {
    SbtLogger.withLogger(log) {
      log.info(s"Initializing instance with file ${schemaFile.getAbsolutePath}")

      val cluster = Cluster.builder()
        .addContactPoint(host)
        .withPort(port)
        .build()
      try {
        val session = cluster.connect()
        IO.read(schemaFile).split(";").foreach { query =>
          val trimmed = query.trim
          if(!trimmed.isEmpty) {
            session.execute(trimmed)
          }
        }
        session.close()
      } finally {
        cluster.close()
      }
    }
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


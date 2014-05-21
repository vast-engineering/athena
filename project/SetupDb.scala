import sbt._
import Keys._

import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import com.datastax.driver.core.Cluster
import org.cassandraunit.dataset.cql.AbstractCQLDataSet
import org.cassandraunit.CQLDataLoader

object SetupDB extends Build {

  val setupCassandra = testOptions in Test += Tests.Setup { () =>
    EmbeddedCassandraServerHelper.startEmbeddedCassandra()

    val dataSet = new FileDataSet(baseDirectory.value, "schema.cql", "testks")
    val cluster = new Cluster.Builder().addContactPoints("localhost").withPort(9142).build()
    val session = cluster.connect()
    val dataLoader = new CQLDataLoader(session)
    dataLoader.load(dataSet)
    session.close()
  }

  val stopCassandra = testOptions in Test += Tests.Cleanup { () =>
    EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
  }

  override lazy val settings = super.settings ++ Seq(setupCassandra, stopCassandra)

}

class FileDataSet(baseDir: File, fileName: String, keyspace: String) extends AbstractCQLDataSet(fileName, true, true, keyspace) {
  import java.io.{FileInputStream, InputStream}

  override def getInputDataSetLocation(dataSetLocation: String): InputStream = {
    val path = s"src/test/resources/$dataSetLocation"
    new FileInputStream(new File(baseDir, path))
  }
}

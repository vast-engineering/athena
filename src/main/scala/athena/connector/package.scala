package athena

import java.net.InetAddress
import athena.Requests.Statement
import athena.data.{PreparedStatementDef, DataType}

package object connector {

//  private[connector] case class ClusterMetadata(name: String,
//                                                hosts: Map[InetAddress, Host],
//                                                keyspaces: Map[String, KeyspaceMetadata])

  //sent amongst the actors to signal that a statement has been prepared (and thus may need to be initialized on other objects)
  private[connector] case class StatementPrepared(stmtDef: PreparedStatementDef)

}

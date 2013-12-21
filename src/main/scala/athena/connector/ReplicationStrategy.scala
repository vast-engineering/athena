package athena.connector

import athena.connector.ClusterInfo.Host

private[connector] sealed abstract class ReplicationStrategy {
  def computeReplicaMap(tokenMap: Map[Token, Host], ring: Seq[Token]): Map[Token, Set[Host]]
}
object ReplicationStrategy {

  def apply(className: String, options: Map[String, String]): Option[ReplicationStrategy] = {
    if (className.contains("SimpleStrategy")) {
      options.get("replication_factor").map { repFactor =>
        new BasicStrategy(repFactor.toInt)
      }
    } else if(className.contains("NetworkTopologyStrategy")) {
      Some(new NetworkTopologyStrategy(options.mapValues(_.toInt)))
    } else {
      None
    }
  }

}
private[connector] class BasicStrategy(replicationFactor: Int) extends ReplicationStrategy {
  def computeReplicaMap(tokenMap: Map[Token, Host], ring: Seq[Token]): Map[Token, Set[Host]] = ???
}

private[connector] class NetworkTopologyStrategy(replicationFactors: Map[String, Int]) extends ReplicationStrategy {
  def computeReplicaMap(tokenMap: Map[Token, Host], ring: Seq[Token]): Map[Token, Set[Host]] = ???
}


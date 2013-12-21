package athena

import java.net.InetAddress

object TestData {

  val Hosts = Set(
    InetAddress.getByName("cassandra1.oak.vast.com"),
    InetAddress.getByName("cassandra3.oak.vast.com"),
    InetAddress.getByName("cassandra6.oak.vast.com")
  )

//  val Hosts = Set(
//    InetAddress.getByName("127.0.0.1"),
//    InetAddress.getByName("127.0.0.2"),
//    InetAddress.getByName("127.0.0.3")
//  )


  val Port = 9042


}

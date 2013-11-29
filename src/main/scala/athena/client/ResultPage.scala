package athena.client

import athena.Responses.Rows
import akka.event.Logging
import athena.data.CValue
import scala.concurrent.Future

//TODO: Thoughts about this - perhaps offer a version of a ResultSet that acts as
//an Enumerator, and then provide some simple iteratees to consume rows.
//For now, this API is good (and simple) enough

case class Row(columns: IndexedSeq[CValue])

/**
 * This class models a single page of results from a query. Note that the rows Iterator does not necessarily
 * contain *all* of the possible data returned from the query, just the current page. To fully exhaust the results,
 * you should iterate over the
 */
class ResultPage(rawRows: Rows, connection: Connection) {
  private[this] val log = Logging(connection.system, this.getClass)

  import connection.system.dispatcher

  def rows: Iterator[Row] = {

    val columnDefs = rawRows.columnDefs

    rawRows.data.iterator.map { rawRow =>
      val columnData = rawRow.zip(columnDefs).map {
        case (data, columnDef) =>
          columnDef.dataType.decode(data)
      }
      Row(columnData)
    }
  }

  def nextPage(): Future[Option[ResultPage]] = {
    rawRows.pagingState.map { ps =>
      connection.loadPage(rawRows.request, ps).map(r => Some(new ResultPage(r, connection)))
    } getOrElse {
      Future.successful(None)
    }
  }


}

import java.io._
import java.sql._
import scala.collection.mutable.{ListBuffer, ListMap}

abstract class Table[T <: Product](val conn: Connection, val tbl: String) {
  def columnNamesWithoutId: List[String]
  def createEntity(rs: ResultSet): T
  def entityId(entity: T): Int

  def all: List[T] = {
    val query = "select * from " + tbl
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(query)
      val buf = scala.collection.mutable.ListBuffer[T]()
      while (rs.next()) buf += createEntity(rs)
      buf.toList
    } finally {
      stmt.close()
    }
  }

  protected def insertImpl(values: Any*): T = {
    val query = "insert into coffees(" + columnNamesWithoutId.mkString(", ") + ") values (" + columnNamesWithoutId.map(_ => "?").mkString(", ") +  ")"
    val stmt = conn.prepareStatement(query)
    try {
      for (i <- 1 to columnNamesWithoutId.length) stmt.setObject(i, values(i - 1))
      stmt.executeUpdate()
      all.last // hack
    } finally {
      stmt.close()
    }
  }

  // TODO: to be defined in concrete ancestors
  // def insert(name1: type1, ... nameN: typeN): T = {
  //   super.insertImpl(name1, ... nameN)
  // }

  def update(entity: T): Unit = {
    val query = "update coffees set " + columnNamesWithoutId.map(_ + " = ?").mkString(", ") + " where id = ?"
    val stmt = conn.prepareStatement(query)
    try {
      val values = entity.productIterator.toList
      for (i <- 1 to columnNamesWithoutId.length) stmt.setObject(i, values(i))
      stmt.setInt(columnNamesWithoutId.length + 1, entityId(entity))
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }

  def remove(entity: T): Unit = {
    val query = "delete from " + tbl + " where id = ?"
    val stmt = conn.prepareStatement(query)
    try {
      stmt.setInt(1, entityId(entity))
      stmt.executeUpdate()
    } finally {
      stmt.close()
    }
  }
}

object Jdbc {
  def list(conn: Connection, query: String): List[ListMap[String, Any]] = {
    val stmt = conn.createStatement()
    try {
      val rs = stmt.executeQuery(query)
      val buf = ListBuffer[ListMap[String, Any]]()
      while (rs.next()) {
        val record = ListMap[String, Any]()
        for (i <- 1 to rs.getMetaData.getColumnCount) {
          record += (rs.getMetaData.getColumnName(i).toLowerCase -> rs.getObject(i))
        }
        buf += record
      }
      buf.toList
    } finally {
      stmt.close()
    }
  }
}

import scala.reflect.macros.Context
import language.experimental.macros
import java.sql._
import java.io.File

object Macros {
  def impl(c: Context)(url: c.Expr[String]) = {
    import c.universe._
    import Flag._

    def projectRoot = {
      val currentFile = c.enclosingUnit.source.file.path
      var projectRoot = new File(currentFile).getParent
      def coffeesDir = new File(projectRoot, "coffees")
      while (!coffeesDir.exists) {
        val projectRoot1 = new File(projectRoot).getParent
        if (projectRoot == projectRoot1) c.abort(c.enclosingPosition, "cannot detect SBT project root")
        projectRoot = projectRoot1
      }
      projectRoot
    }

    def connectionString(): String = {
      val Expr(Literal(Constant(sUrl: String))) = url
      val pathToDb = projectRoot + "/coffees/" + sUrl + ".h2.db"
      "jdbc:h2:" + pathToDb.stripSuffix(".h2.db")
    }

    def generateCodeForTables(): List[Tree] = {
      Class.forName("org.h2.Driver")
      val conn = DriverManager.getConnection(connectionString(), "sa", "")
      try {
        val tables = Jdbc.list(conn, "show tables").map(_("table_name").asInstanceOf[String].toLowerCase)
        tables.flatMap(tbl => {
          // load table schema
          val columns = Jdbc.list(conn, "show columns from " + tbl).map(row => row("column_name").asInstanceOf[String].toLowerCase -> row("type").asInstanceOf[String])
          val schema = columns map { case (name, tpe) => name -> (tpe match {
            case s if s.startsWith("INTEGER") => Ident(TypeName("Int"))
            case s if s.startsWith("VARCHAR") => Ident(TypeName("String"))
            case s if s.startsWith("DOUBLE") => Ident(TypeName("Double"))
          }) }
          val schemaWithoutId = schema filter { case (name, _) => name != "id" }

          // generate the dto case class
          val CASEACCESSOR = scala.reflect.internal.Flags.CASEACCESSOR.asInstanceOf[Long].asInstanceOf[FlagSet]
          val PARAMACCESSOR = scala.reflect.internal.Flags.PARAMACCESSOR.asInstanceOf[Long].asInstanceOf[FlagSet]
          val fields: List[Tree] = schema map { case (name, tpt) => ValDef(Modifiers(CASEACCESSOR | PARAMACCESSOR), name, tpt, EmptyTree) }
          val ctorParams: List[ValDef] = schema map { case (name, tpt) => ValDef(Modifiers(PARAM | PARAMACCESSOR), name, tpt, EmptyTree) }
          val ctor: Tree = DefDef(Modifiers(), nme.CONSTRUCTOR, List(), List(ctorParams), TypeTree(), Block(List(pendingSuperCall), Literal(Constant(()))))
          val caseClass = ClassDef(Modifiers(CASE), TypeName(tbl.capitalize.dropRight(1)), Nil, Template(
            List(Select(Ident(TermName("scala")), TypeName("Product")), Select(Ident(TermName("scala")), TypeName("Serializable"))),
            emptyValDef,
            fields :+ ctor))

          // generate the table object
          val tableSuper = Apply(AppliedTypeTree(Ident(TypeName("Table")), List(Ident(caseClass.name))), List(Ident(TermName("conn")), Literal(Constant(tbl))))
          val columnNamesWithoutId = schemaWithoutId map { case (name, _) => Literal(Constant(name)) }
          val resultSet = Select(Select(Ident(TermName("java")), TermName("sql")), TypeName("ResultSet"))
          val createEntity = Apply(Ident(caseClass.name.toTermName), schema map {
            case (name, tpe) => Apply(Select(Ident(TermName("rs")), "get" + tpe.name.toString), List(Literal(Constant(name))))
          })
          val insertParams = schemaWithoutId map { case (name, tpt) => ValDef(Modifiers(PARAM), name, tpt, EmptyTree) }
          val insertArgs = schemaWithoutId map { case (name, _) => Ident(TermName(name)) }
          val tableModule = ModuleDef(NoMods, TermName(tbl.capitalize), Template(List(tableSuper), emptyValDef, List(
            DefDef(Modifiers(), nme.CONSTRUCTOR, List(), List(List()), TypeTree(), Block(List(pendingSuperCall), Literal(Constant(())))),
            DefDef(Modifiers(), TermName("columnNamesWithoutId"), List(), List(), TypeTree(), Block(List(), Apply(Ident(TermName("List")), columnNamesWithoutId))),
            DefDef(Modifiers(), TermName("createEntity"), List(), List(List(ValDef(Modifiers(PARAM), TermName("rs"), resultSet, EmptyTree))), TypeTree(), Block(List(), createEntity)),
            DefDef(Modifiers(), TermName("entityId"), List(), List(List(ValDef(Modifiers(PARAM), TermName("entity"), Ident(caseClass.name), EmptyTree))), TypeTree(), Block(List(), Select(Ident(TermName("entity")), TermName("id")))),
            DefDef(Modifiers(), TermName("insert"), List(), List(insertParams), TypeTree(), Block(List(), Apply(Select(Super(This(tpnme.EMPTY), tpnme.EMPTY), TermName("insertImpl")), insertArgs)))
          )))

          // that's it
          List(caseClass, tableModule)
        })
      } finally {
        conn.close()
      }
    }

    val Expr(Block(List(ClassDef(_, _, _, Template(parents, self, body))), _)) = reify {
      class CONTAINER {
        Class.forName("org.h2.Driver")
        val conn = DriverManager.getConnection(c.literal(connectionString()).splice, "sa", "")
        // generated code will be spliced here
      }
    }

    val packageName = c.enclosingPackage.pid.toString
    val className = c.freshName(c.enclosingImpl.name).toTypeName
    c.introduceTopLevel(packageName, ClassDef(NoMods, className, Nil, Template(parents, self, body ++ generateCodeForTables())))
    Select(c.enclosingPackage.pid, className)
  }

  type H2Db(url: String) = macro impl
}
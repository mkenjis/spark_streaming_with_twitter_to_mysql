// mkdir /SqlProducer

// SqlProducerWrapper.scala

package sqlProducerWrapper

import java.sql.{Connection,DriverManager,PreparedStatement}

class SqlProducerWrapper(url: String,user: String,password: String) {

  Class.forName("com.mysql.jdbc.Driver")
  val conn = DriverManager.getConnection(url,user,password)
  val stmt = conn.createStatement()

  def send(key: String, value: String) {
     stmt.addBatch("insert into df_aux(cp1,cp2) values('"+key+"','"+value+"')")
  }
  
  def execBatch(key: String) {
     stmt.executeBatch()
  }
}

object SqlProducerWrapper {
  var url = ""
  var user = ""
  var password = ""
  lazy val instance = new SqlProducerWrapper(url,user,password)
}

----------------------

// sqlproducer.sbt

name := "sql-producer"

version := "1.0.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-streaming" % "2.3.2" % "provided"
)

-----build jar file with sbt ----------

cd KfkProducer
sbt package
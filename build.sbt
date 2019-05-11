name := "ScalaTest"

version := "0.1"

scalaVersion := "2.11.12"
resolvers += Resolver.url("SparkPackages", url("https://dl.bintray.com/spark-packages/maven/"))
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "org.apache.spark" %% "spark-graphx" % "2.3.1",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.spark" %% "spark-hive" % "2.3.1",
  "org.neo4j" % "neo4j" % "3.3.1",
  "org.neo4j.driver" % "neo4j-java-driver" % "1.4.4",
  "org.vegas-viz" %% "vegas" % "0.3.11",
  "org.vegas-viz" %% "vegas-spark" % "0.3.11",
  "com.crealytics" %% "spark-excel" % "0.11.1"
)

name := "ScalaTest"

version := "0.1"

scalaVersion := "2.11.12"
resolvers += Resolver.url("SparkPackages", url("https://dl.bintray.com/spark-packages/maven/"))
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.1",
  "org.apache.spark" %% "spark-sql" % "2.3.1",
  "org.apache.spark" %% "spark-graphx" % "2.3.1"
  


)

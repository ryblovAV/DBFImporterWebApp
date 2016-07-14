name := "DBFImporterWebApp"

version := "1.0"

lazy val `dbfimporterwebapp` = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

libraryDependencies ++= Seq( jdbc , cache , ws   , specs2 % Test )

libraryDependencies ++= Seq(
  "org.springframework" % "spring-core" % "4.1.1.RELEASE",
  "org.springframework" % "spring-context" % "4.1.1.RELEASE",
  "org.springframework" % "spring-beans" % "4.1.1.RELEASE",
  "org.springframework" % "spring-jdbc" % "4.1.1.RELEASE"
)

libraryDependencies += "org.samba.jcifs" % "jcifs" % "1.3.14-kohsuke-1"

libraryDependencies += "com.typesafe.akka" % "akka-actor_2.11" % "2.4.5"


libraryDependencies ++= Seq(
  "org.apache.poi" % "poi" % "3.13",
  "org.apache.poi" % "poi-ooxml" % "3.13"
)

unmanagedResourceDirectories in Test <+=  baseDirectory ( _ /"target/web/public/test" )  

resolvers += "scalaz-bintray" at "https://dl.bintray.com/scalaz/releases"  
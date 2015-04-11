import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object KafkaUtilsBuild extends Build {

  val mavenPublishSettings : Seq[sbt.Project.Setting[_]] = Seq(
    publishMavenStyle := true,
    publishTo <<= isSnapshot {
      (ss: Boolean) =>
        val nexus = "https://maven.visualdna.com/nexus/"
        if (ss)
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases" at nexus + "content/repositories/releases")
    },
    credentials ++= {
      val sonatype = ("Sonatype Nexus Repository Manager", "maven.visualdna.com")
      def loadMavenCredentials(file: java.io.File): Seq[Credentials] = {
        xml.XML.loadFile(file) \ "servers" \ "server" map (s => {
          val host = (s \ "id").text
          val realm = sonatype._1
          val hostToUse = "maven.visualdna.com"
          Credentials(realm, hostToUse, (s \ "username").text, (s \ "password").text)
        })
      }
      val ivyCredentials = Path.userHome / ".ivy2" / ".credentials"
      val mavenCredentials = Path.userHome / ".m2" / "settings.xml"
      (ivyCredentials.asFile, mavenCredentials.asFile) match {
        case (ivy, _) if ivy.canRead => Credentials(ivy) :: Nil
        case (_, mvn) if mvn.canRead => loadMavenCredentials(mvn)
        case _ => Nil
      }
    }
  )

  def sharedSettings = Defaults.defaultSettings ++ assemblySettings ++ Seq(
    version := "0.2.2-VDNA-1-SNAPSHOT",
    scalaVersion := "2.10.3",
    organization := "com.quantifind",
    scalacOptions := Seq("-deprecation", "-unchecked", "-optimize"),
    unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
    retrieveManaged := true,
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),
    resolvers ++= Seq(
      "sonatype-snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
      "sonatype-releases" at "http://oss.sonatype.org/content/repositories/releases",
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/"),
    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.17",
      "org.scalatest" %% "scalatest" % "1.9.1" % "test",
      "org.apache.kafka" %% "kafka" % "0.8.1"))

  val slf4jVersion = "1.6.1"

  //offsetmonitor project

  lazy val offsetmonitor = Project("offsetmonitor", file("."), settings = offsetmonSettings)

  def offsetmonSettings = sharedSettings ++ mavenPublishSettings ++ Seq(
    name := "KafkaOffsetMonitor",
    libraryDependencies ++= Seq(
      "net.databinder" %% "unfiltered-filter" % "0.6.7",
      "net.databinder" %% "unfiltered-jetty" % "0.6.7",
      "net.databinder" %% "unfiltered-json" % "0.6.7",
      "com.quantifind" %% "sumac" % "0.3.0",
      "com.typesafe.slick" %% "slick" % "2.0.0",
      "org.xerial" % "sqlite-jdbc" % "3.7.2",
      "com.twitter" % "util-core" % "3.0.0"),
    resolvers ++= Seq(
      "java m2" at "http://download.java.net/maven/2",
      "twitter repo" at "http://maven.twttr.com"))
}

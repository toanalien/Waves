import com.typesafe.sbt.packager.archetypes.TemplateWriter
import sbt.Keys._
import sbt._

fork in run := true

enablePlugins(JavaServerAppPackaging, JDebPackaging, SystemdPlugin, GitVersioning)

inThisBuild(Seq(
  scalaVersion := "2.12.4",
  organization := "com.wavesplatform",
  crossPaths := false
))

name := "waves"

git.useGitDescribe := true
git.uncommittedSignifier := Some("DIRTY")


publishArtifact in (Compile, packageDoc) := false
publishArtifact in (Compile, packageSrc) := false
mainClass in Compile := Some("com.wavesplatform.Application")
scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-Ywarn-unused:-implicits",
  "-Xlint")
logBuffered := false

resolvers += Resolver.bintrayRepo("ethereum", "maven")

//assembly settings
assemblyJarName in assembly := s"waves-all-${version.value}.jar"
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.concat
  case other => (assemblyMergeStrategy in assembly).value(other)
}
test in assembly := {}

libraryDependencies ++=
  Dependencies.network ++
  Dependencies.db ++
  Dependencies.http ++
  Dependencies.akka ++
  Dependencies.serialization ++
  Dependencies.testKit.map(_ % "test") ++
  Dependencies.logging ++
  Dependencies.matcher ++
  Dependencies.metrics ++
  Dependencies.fp ++
  Seq(
    "com.iheart" %% "ficus" % "1.4.2",
    ("org.scorexfoundation" %% "scrypto" % "1.2.2").exclude("org.slf4j", "slf4j-api"),
    "commons-net" % "commons-net" % "3.+"
  )

sourceGenerators in Compile += Def.task {
  val versionFile = (sourceManaged in Compile).value / "com" / "wavesplatform" / "Version.scala"
  val versionExtractor = """(\d+)\.(\d+)\.(\d+).*""".r
  val versionExtractor(major, minor, bugfix) = version.value
  IO.write(versionFile,
    s"""package com.wavesplatform
      |
      |object Version {
      |  val VersionString = "${version.value}"
      |  val VersionTuple = ($major, $minor, $bugfix)
      |}
      |""".stripMargin)
  Seq(versionFile)
}

inConfig(Test)(Seq(
  logBuffered := false,
  parallelExecution := false,
  testOptions += Tests.Argument("-oIDOF", "-u", "target/test-reports"),
  testOptions += Tests.Setup(_ => sys.props("sbt-testing") = "true")
))

commands += Command.command("packageAll") { state =>
  "clean" ::
  "assembly" ::
  "debian:packageBin" ::
  state
}

inConfig(Linux)(Seq(
  maintainer := "wavesplatform.com",
  packageSummary := "Waves node",
  packageDescription := "Waves node"
))

val network = Def.setting { Network(sys.props.get("network")) }
normalizedName := network.value.name

javaOptions in Universal ++= Seq(
  // -J prefix is required by the bash script
  "-J-server",
  // JVM memory tuning for 2g ram
  "-J-Xms128m",
  "-J-Xmx2g",
  "-J-XX:+ExitOnOutOfMemoryError",
  // Java 9 support
  "-J-XX:+IgnoreUnrecognizedVMOptions",
  "-J--add-modules=java.xml.bind",

  // from https://groups.google.com/d/msg/akka-user/9s4Yl7aEz3E/zfxmdc0cGQAJ
  "-J-XX:+UseG1GC",
  "-J-XX:+UseNUMA",
  "-J-XX:+AlwaysPreTouch",

  // probably can't use these with jstack and others tools
  "-J-XX:+PerfDisableSharedMem",
  "-J-XX:+ParallelRefProcEnabled",
  "-J-XX:+UseStringDeduplication")

mappings in Universal += (baseDirectory.value / s"waves-${network.value}.conf" -> "doc/waves.conf.sample")
val packageSource = Def.setting { sourceDirectory.value / "package" }
val upstartScript = Def.task {
  val src = packageSource.value / "upstart.conf"
  val dest = (target in Debian).value / "upstart" / s"${packageName.value}.conf"
  val result = TemplateWriter.generateScript(src.toURI.toURL, linuxScriptReplacements.value)
  IO.write(dest, result)
  dest
}
linuxPackageMappings ++= Seq(
  (upstartScript.value, s"/etc/init/${packageName.value}.conf")
).map(packageMapping(_).withConfig().withPerms("644"))

linuxStartScriptTemplate in Debian := (packageSource.value / "systemd.service").toURI.toURL
linuxScriptReplacements += "detect-loader" ->
  """is_systemd() {
    |    which systemctl >/dev/null 2>&1 && \
    |    systemctl | grep -- -\.mount >/dev/null 2>&1
    |}
    |is_upstart() {
    |    /sbin/init --version | grep upstart >/dev/null 2>&1
    |}
    |""".stripMargin

inConfig(Debian)(Seq(
  debianPackageDependencies += "java8-runtime-headless",
  serviceAutostart := false,
  maintainerScripts := maintainerScriptsFromDirectory(packageSource.value / "debian", Seq("preinst", "postinst", "postrm", "prerm"))
))

lazy val node = project.in(file("."))

lazy val discovery = project

lazy val it = project
  .dependsOn(node)

lazy val generator = project
  .dependsOn(it)
  .settings(
    libraryDependencies += "com.github.scopt" %% "scopt" % "3.6.0"
  )




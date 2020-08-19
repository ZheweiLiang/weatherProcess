logLevel := Level.Warn

resolvers += Resolver.url("bintray-kipsigman-sbt-plugins", url("http://dl.bintray.com/kipsigman/sbt-plugins"))(Resolver.ivyStylePatterns)
resolvers += "Flyway" at "https://flywaydb.org/repo"

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.3")

addSbtPlugin("com.geirsson" % "sbt-scalafmt" % "1.3.0")
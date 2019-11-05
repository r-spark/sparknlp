spark_dependencies <- function(spark_version, scala_version, ...) {
  sparklyr::spark_dependency(
    jars = c(
     system.file(
       sprintf("java/sparknlp-%s-%s.jar", spark_version, scala_version),
       package = "sparknlp"
     )
    ),
    packages = c(
      sprintf("com.johnsnowlabs.nlp:spark-nlp_2.11:2.3.1")
    )
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}

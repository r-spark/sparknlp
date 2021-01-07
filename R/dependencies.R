spark_nlp_version <- "2.6.5"

spark_dependencies <- function(spark_version, scala_version, ...) {
  secretCode <- Sys.getenv("SPARK_NLP_SECRET_CODE", unset = NA)
  
  if (is.na(secretCode)) {
    sparklyr::spark_dependency(
      jars = c(
        system.file(
          sprintf("java/sparknlp-%s-%s.jar", spark_version, scala_version),
          package = "sparknlp"
        )
      ),
      packages = c(
        sprintf("com.johnsnowlabs.nlp:spark-nlp_2.11:%s", spark_nlp_version)
      )
    )    
  } else {
    jsl_version <- strsplit(secretCode, "-")[[1]][1]
    jsl_url <- paste0("https://pypi.johnsnowlabs.com/", secretCode)
    
    sparklyr::spark_dependency(
      jars = c(
        system.file(
          sprintf("java/sparknlp-%s-%s.jar", spark_version, scala_version),
          package = "sparknlp"
        )
      ),
      packages = c(
        sprintf("com.johnsnowlabs.nlp:spark-nlp_2.11:%s", spark_nlp_version),
                "com.johnsnowlabs.nlp:spark-nlp-jsl:2.7.2"
      ),
      repositories = c(jsl_url)
    )  
  }
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}

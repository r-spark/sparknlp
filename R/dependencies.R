spark_nlp_version <- "2.7.3"

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
    jsl_url <- paste0("https://pypi.johnsnowlabs.com/", secretCode, "/spark-nlp-jsl-", jsl_version, ".jar")
    
    sparklyr::spark_dependency(
      jars = c(
        system.file(
          sprintf("java/sparknlp-%s-%s.jar", spark_version, scala_version),
          package = "sparknlp"
        ),
        jsl_url
      ),
      packages = c(
        sprintf("com.johnsnowlabs.nlp:spark-nlp_2.11:%s", spark_nlp_version)
      )
    )  
  }
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}

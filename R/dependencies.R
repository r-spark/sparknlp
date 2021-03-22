spark_nlp_version <- "3.0.0"

spark_dependencies <- function(spark_version, scala_version, ...) {
  secretCode <- Sys.getenv("SPARK_NLP_SECRET_CODE", unset = NA)
  
  # Determine the Spark version so that we insert the correct jar from Maven
  spark_version_parts <- strsplit(spark_version, "\\.")[[1]]

  if (spark_version_parts[1] == "2" & spark_version_parts[2] == "3") {
    artifact_id <- sprintf("spark-nlp-spark23_%s", scala_version)
  } else if (spark_version_parts[1] == "2" & spark_version_parts[2] == "4") {
    artifact_id <- sprintf("spark-nlp-spark24_%s", scala_version)    
  } else if (spark_version_parts[1] == "3") {
    artifact_id <- sprintf("spark-nlp_%s", scala_version)
  } else {
    stop(sprintf("Incompatible versions of Spark (%s), Scala (%s) and Spark NLP (%s)!", 
                 spark_version, scala_version, spark_nlp_version))
  }

  # Determine if the JSL license is setup
  if (is.na(secretCode)) {
    sparklyr::spark_dependency(
      jars = c(
        system.file(
          sprintf("java/sparknlp-%s-%s.jar", spark_version, scala_version),
          package = "sparknlp"
        )
      ),
      packages = c(
        sprintf("com.johnsnowlabs.nlp:spark-nlp_%s:%s", scala_version, spark_nlp_version)
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
        sprintf("com.johnsnowlabs.nlp:spark-nlp_%s:%s", scala_version, spark_nlp_version)
      )
    )  
  }
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}

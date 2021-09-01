spark_nlp_version <- "3.2.1"

spark_jsl_version <- function() {
  secretCode <- Sys.getenv("SPARK_NLP_SECRET_CODE", unset = NA)

  if (!is.na(secretCode)) {
    jsl_version <- strsplit(secretCode, "-")[[1]][1]
    return(jsl_version)    
  } else {
    return(NA)
  }
}

spark_dependencies <- function(spark_version, scala_version, ...) {
  jsl_version <- spark_jsl_version()
  gpu <- as.logical(Sys.getenv("SPARK_NLP_GPU", unset = FALSE))
  
  if (!is.na(gpu) & gpu) {
    gpu = "-gpu"
  } else {
    gpu = ""
  }

  # Determine the Spark version so that we insert the correct jar from Maven
  if (spark_version == 2.3) {
    artifact_id <- sprintf("spark-nlp%s-spark23_%s", gpu, scala_version)
  } else if (spark_version == 2.4) {
    artifact_id <- sprintf("spark-nlp%s-spark24_%s", gpu, scala_version)    
  } else if (spark_version >= 3) {
    artifact_id <- sprintf("spark-nlp%s_%s", gpu, scala_version)
  } else {
    stop(sprintf("Incompatible versions of Spark (%s), Scala (%s) and Spark NLP (%s)!", 
                 spark_version, scala_version, spark_nlp_version))
  }

  # Determine if the JSL license is setup
  if (is.na(jsl_version)) {
    sparklyr::spark_dependency(
      jars = c(
        system.file(
          sprintf("java/sparknlp-%s-%s.jar", spark_version, scala_version),
          package = "sparknlp"
        )
      ),
      packages = c(
        sprintf("com.johnsnowlabs.nlp:%s:%s", artifact_id, spark_nlp_version)
      )
    )    
  } else {
    secretCode <- Sys.getenv("SPARK_NLP_SECRET_CODE")
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
        sprintf("com.johnsnowlabs.nlp:%s:%s", artifact_id, spark_nlp_version)
      )
    )  
  }
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}

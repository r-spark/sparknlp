library(testthat)
library(sparklyr)
library(sparknlp)

if (identical(Sys.getenv("NOT_CRAN"), "true")) {
  test_check("sparknlp")
  on.exit({spark_disconnect_all()})
}


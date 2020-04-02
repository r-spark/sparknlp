#!/usr/bin/env Rscript

library(purrr)
library(dplyr)

spec <- sparklyr::spark_default_compilation_spec() %>%
  map(function(x) {
    x$jar_dep <- list.files("~/.m2/repository/com/johnsnowlabs/nlp/spark-nlp_2.11/2.4.5/", full.names = TRUE) %>% 
      map_chr(normalizePath)
    x
  }) %>%
  keep(~ .x$spark_version >= "2.4.0")

sparklyr::compile_package_jars(spec = spec)

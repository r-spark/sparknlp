#!/usr/bin/env Rscript

library(purrr)
library(dplyr)

pkg <- sparklyr:::infer_active_package_name()

spec <- sparklyr::spark_compilation_spec(jar_name = sprintf("%s-2.4-2.11.jar", pkg), 
                                         jar_path = sparklyr:::find_jar())

#  map(function(x) {
spec$jar_dep <- list.files("~/.m2/repository/com/johnsnowlabs/nlp/spark-nlp_2.11/2.5.5/", full.names = TRUE) %>% 
    map_chr(normalizePath)
#    x
#  })
#  keep(~ .x$spark_version >= "2.4.3")

# For some reason this is needed for ContextSpellChecker
#spec <- spec %>% 
#  map(function(x) {
spec$jar_dep <- append(spec$jar_dep, list.files("~/.m2/repository/com/github/universal-automata/liblevenshtein/3.0.0/", full.names = TRUE) %>% 
      map_chr(normalizePath))
#    x
#  }) %>% 
#  keep(~ .x$spark_version >= "2.4.0")

sparklyr::compile_package_jars(spec = spec)



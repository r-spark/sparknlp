#!/usr/bin/env Rscript

library(purrr)
library(dplyr)

scalac_212_path <- path.expand("~/apps/scala-2.12.11/bin/scalac")
spark_nlp_211_version <- "2.7.3"
spark_nlp_212_version <- "3.3.0"

spark_version_24 <- "2.4.7"
spark_version_30 <- "3.0.2"
spark_version_31 <- "3.1.1"

pkg <- sparklyr:::infer_active_package_name()

# 2.4 won't compile anymore because of changes to method names in ContextSpellChecker
# Compile for Scala 2.11/Spark 2.4
#spec <- sparklyr::spark_compilation_spec(jar_name = sprintf("%s-2.4-2.11.jar", pkg), 
#                                         jar_path = sparklyr:::find_jar(),
#                                         spark_version = spark_version_24)

#spec$jar_dep <- list.files(sprintf("~/.m2/repository/com/johnsnowlabs/nlp/spark-nlp_2.11/%s/", spark_nlp_211_version),
#                           full.names = TRUE) %>% 
#    map_chr(normalizePath)

# For some reason this is needed for ContextSpellChecker
#spec$jar_dep <- append(spec$jar_dep, list.files("~/.m2/repository/com/github/universal-automata/liblevenshtein/3.0.0/", 
#                                                full.names = TRUE) %>% 
#      map_chr(normalizePath))

#sparklyr::compile_package_jars(spec = spec)

# Compile for Scala 2.12/Spark 3.0
spec_212_30 <- sparklyr::spark_compilation_spec(jar_name = sprintf("%s-3.0-2.12.jar", pkg), 
                                         jar_path = sparklyr:::find_jar(),
                                         spark_version = spark_version_30,
                                         scalac_path = scalac_212_path)

spec_212_30$jar_dep <- list.files(sprintf("~/.m2/repository/com/johnsnowlabs/nlp/spark-nlp_2.12/%s/", spark_nlp_212_version), 
                                  full.names = TRUE) %>% 
  map_chr(normalizePath)

spec_212_30$jar_dep <- append(spec_212_30$jar_dep, list.files("~/.m2/repository/com/github/universal-automata/liblevenshtein/3.0.0/", full.names = TRUE) %>% 
       map_chr(normalizePath))

sparklyr::compile_package_jars(spec = spec_212_30)

# Compile for Scala 2.12/Spark 3.1
spec_212_31 <- sparklyr::spark_compilation_spec(jar_name = sprintf("%s-3.1-2.12.jar", pkg), 
                                             jar_path = sparklyr:::find_jar(),
                                             spark_version = spark_version_31,
                                             scalac_path = "/home/davek/apps/scala-2.12.11/bin/scalac")

spec_212_31$jar_dep <- list.files(sprintf("~/.m2/repository/com/johnsnowlabs/nlp/spark-nlp_2.12/%s/", spark_nlp_212_version),
                                  full.names = TRUE) %>% 
  map_chr(normalizePath)

spec_212_31$jar_dep <- append(spec_212_31$jar_dep, list.files("~/.m2/repository/com/github/universal-automata/liblevenshtein/3.0.0/", full.names = TRUE) %>% 
                             map_chr(normalizePath))

sparklyr::compile_package_jars(spec = spec_212_31)


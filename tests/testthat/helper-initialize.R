"%||%" <- function(x, y) {
  if (is.null(x)) y else x
}

#test_text <- data.frame(text = c("In 1949, Grace Hopper became an employee of the Eckert–Mauchly Computer Corporation as a senior mathematician and joined the team developing the UNIVAC I. Hopper also served as UNIVAC director of Automatic Programming Development for Remington Rand.",
#               "Turing is widely considered to be the father of theoretical computer science and artificial intelligence. During the Second World War, Turing was a leading participant in the breaking of German ciphers at Bletchley Park."))

test_text <- data.frame(text = c("The cats are laying in front of the fireplace.",
                                 "The dogs are staying cool in the kitchen."))

test_classifier_text <- data.frame(description = c("The cats are laying in front of the fireplace.",
                                                   "The dogs are staying cool in the kitchen."),
                                   category = c("Business", "Pets"))

# helper functions from sparklyr tests
# https://github.com/rstudio/sparklyr/blob/master/tests/testthat/helper-initialize.R
testthat_spark_connection <- function() {
  version <- Sys.getenv("SPARK_VERSION", unset = "2.4.3")
  
  spark_installed <- sparklyr::spark_installed_versions()
  if (nrow(spark_installed[spark_installed$spark == version, ]) == 0) {
    options(sparkinstall.verbose = TRUE)
    sparklyr::spark_install(version)
  }
  
  # generate connection if none yet exists
  connected <- FALSE
  if (exists(".testthat_spark_connection", envir = .GlobalEnv)) {
    sc <- get(".testthat_spark_connection", envir = .GlobalEnv)
    connected <- sparklyr::connection_is_open(sc)
  }
  
  if (!connected) {
    config <- sparklyr::spark_config()
    config$`sparklyr.shell.driver-memory` <- "16G"

    secretCode <- Sys.getenv("SPARK_NLP_SECRET_CODE", unset = NA)
    
    if (!is.na(secretCode)) {
      jsl_version <- strsplit(secretCode, "-")[[1]][1]
      jsl_url <- paste0("https://pypi.johnsnowlabs.com/", secretCode, "/spark-nlp-jsl-", jsl_version, ".jar")
      config$`spark.jars` <- c(jsl_url)
    }
    
    options(sparklyr.sanitize.column.names.verbose = TRUE)
    options(sparklyr.verbose = TRUE)
    options(sparklyr.na.omit.verbose = TRUE)
    options(sparklyr.na.action.verbose = TRUE)
    
    sc <- sparklyr::spark_connect(master = "local", version = version, config = config)
    assign(".testthat_spark_connection", sc, envir = .GlobalEnv)
  }

  # retrieve spark connection
  get(".testthat_spark_connection", envir = .GlobalEnv)
}

testthat_tbl <- function(name) {
  sc <- testthat_spark_connection()
  tbl <- tryCatch(dplyr::tbl(sc, name), error = identity)
  if (inherits(tbl, "error")) {
    data <- eval(as.name(name), envir = parent.frame())
    tbl <- dplyr::copy_to(sc, data, name = name)
  }
  tbl
}

skip_unless_verbose <- function(message = NULL) {
  message <- message %||% "Verbose test skipped"
  verbose <- Sys.getenv("SPARKLYR_TESTS_VERBOSE", unset = NA)
  if (is.na(verbose)) skip(message)
  invisible(TRUE)
}

test_requires <- function(...) {
  
  for (pkg in list(...)) {
    if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
      fmt <- "test requires '%s' but '%s' is not installed"
      skip(sprintf(fmt, pkg, pkg))
    }
  }
  
  invisible(TRUE)
}

check_params <- function(test_args, params) {
  purrr::iwalk(
    test_args,
    function(.x, .y) {
      testthat::expect_equal(.x, params[[.y]], info = .y)
    }
  )
}

test_param_setting <- function(sc, fn, test_args) {
  collapse_sublists <- function(x) purrr::map_if(x, rlang::is_bare_list, unlist)

  params1 <- do.call(fn, c(list(x = sc), test_args)) %>%
    ml_params(allow_null = TRUE) %>%
    collapse_sublists()
  
  params2 <- do.call(fn, c(list(x = ml_pipeline(sc)), test_args)) %>%
    ml_stage(1) %>%
    ml_params(allow_null = TRUE) %>%
    collapse_sublists()
  
  test_args <- collapse_sublists(test_args)
  check_params(test_args, params1)
  check_params(test_args, params2)
}

test_default_args <- function(sc, fn) {
  default_args <- rlang::fn_fmls(fn) %>%
    as.list() %>%
    purrr::discard(~ is.symbol(.x) || is.language(.x)) %>%
    rlang::modify(uid = NULL) %>%
    purrr::compact()
  
  params <- do.call(fn, list(x = sc)) %>%
    ml_params(allow_null = TRUE)
  
  check_params(default_args, params)
}
#' Spark NLP RegexMatcher
#'
#' Spark ML estimator that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#regexmatcher}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param strategy Can be any of MATCH_FIRST|MATCH_ALL|MATCH_COMPLETE
#' @param rules_path Path to file containing a set of regex,key pair
#' @param rules_path_delimiter delimiter between regex and key in the file
#' @param rules_path_read_as TEXT or SPARK_DATASET
#' @param rules_path_options options passed to Spark reader if read_as is SPARK_DATASET
#' 
#' @export
nlp_regex_matcher <- function(x, input_cols, output_col,
                 strategy = NULL, rules_path = NULL, rules_path_delimiter = ",", rules_path_read_as = "TEXT", 
                 rules_path_options = list("format" = "text"),
                 uid = random_string("regex_matcher_")) {
  UseMethod("nlp_regex_matcher")
}

#' @export
nlp_regex_matcher.spark_connection <- function(x, input_cols, output_col,
                 strategy = NULL, rules_path = NULL, rules_path_delimiter = ",", 
                 rules_path_read_as = "TEXT", rules_path_options = list("format" = "text"),
                 uid = random_string("regex_matcher_")) {

  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    strategy = strategy,
    rules_path = rules_path,
    rules_path_delimiter = rules_path_delimiter,
    rules_path_read_as = rules_path_read_as,
    rules_path_options = rules_path_options,
    uid = uid
  ) %>%
  validator_nlp_regex_matcher()

  if (!is.null(args[["rules_path_options"]])) {
    args[["rules_path_options"]] <- list2env(args[["rules_path_options"]])
  }
  
  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.RegexMatcher",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setStrategy", args[["strategy"]])
  
  if (!is.null(args[["rules_path"]])) {
    sparklyr::invoke(jobj, "setRules", args[["rules_path"]], args[["rules_path_delimiter"]],
                     read_as(x, args[["rules_path_read_as"]]), args[["rules_path_options"]])
  }

  new_nlp_regex_matcher(jobj)
}

#' @export
nlp_regex_matcher.ml_pipeline <- function(x, input_cols, output_col,
                 strategy = NULL, rules_path = NULL, rules_path_delimiter = ",", rules_path_read_as = "TEXT", rules_path_options = list("format"="text"),
                 uid = random_string("regex_matcher_")) {

  stage <- nlp_regex_matcher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    strategy = strategy,
    rules_path = rules_path,
    rules_path_delimiter = rules_path_delimiter,
    rules_path_read_as = rules_path_read_as,
    rules_path_options = rules_path_options,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_regex_matcher.tbl_spark <- function(x, input_cols, output_col,
                 strategy = NULL, rules_path = NULL, rules_path_delimiter = ",", rules_path_read_as = "TEXT", rules_path_options = list("format"="text"),
                 uid = random_string("regex_matcher_")) {
  stage <- nlp_regex_matcher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    strategy = strategy,
    rules_path = rules_path,
    rules_path_delimiter = rules_path_delimiter,
    rules_path_read_as = rules_path_read_as,
    rules_path_options = rules_path_options,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_regex_matcher <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["strategy"]] <- cast_nullable_string(args[["strategy"]])
  args[["rules_path"]] <- cast_nullable_string(args[["rules_path"]])
  args[["rules_path_delimiter"]] <- cast_nullable_string(args[["rules_path_delimiter"]])
  args[["rules_path_read_as"]] <- cast_choice(args[["rules_path_read_as"]], choices = c("TEXT", "SPARK_DATASET"))
  args
}

new_nlp_regex_matcher <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_regex_matcher")
}

new_nlp_regex_matcher_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_regex_matcher_model")
}

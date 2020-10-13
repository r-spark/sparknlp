#' Spark NLP Normalizer
#'
#' Spark ML estimator that removes all dirty characters from text following a regex pattern and transforms words based on a provided dictionary
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#normalizer}
#' 
#' @template roxlate-nlp-ml-algo
#' @template roxlate-inputs-output-params
#' @param cleanup_patterns  Regular expressions list for normalization, defaults [^A-Za-z]
#' @param lowercase lowercase tokens, default true
#' @param dictionary_path txt file with delimited words to be transformed into something else
#' @param dictionary_delimiter delimiter of the dictionary text file 
#' @param dictionary_read_as LINE_BY_LINE or SPARK_DATASET
#' @param dictionary_options options to pass to the Spark reader
#' 
#' @export
nlp_normalizer <- function(x, input_cols, output_col,
                 cleanup_patterns = NULL, lowercase = NULL, dictionary_path = NULL,
                 dictionary_delimiter = NULL, dictionary_read_as = "LINE_BY_LINE", dictionary_options = list("format" = "text"),
                 uid = random_string("normalizer_")) {
  UseMethod("nlp_normalizer")
}

#' @export
nlp_normalizer.spark_connection <- function(x, input_cols, output_col,
                 cleanup_patterns = NULL, lowercase = NULL, dictionary_path = NULL,
                 dictionary_delimiter = NULL, dictionary_read_as = "LINE_BY_LINE", dictionary_options = list("format" = "text"),
                 uid = random_string("normalizer_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    cleanup_patterns = cleanup_patterns,
    lowercase = lowercase,
    dictionary_path = dictionary_path,
    dictionary_delimiter = dictionary_delimiter,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    uid = uid
  ) %>%
  validator_nlp_normalizer()
  
  

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.Normalizer",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setCleanupPatterns", args[["cleanup_patterns"]])  %>%
    sparklyr::jobj_set_param("setLowercase", args[["lowercase"]])  %>%
    sparklyr::jobj_set_param("setSlangDictionary", args[["dictionary_path"]]) 
  
  if (!is.null(args[["dictionary_path"]])) {
    sparklyr::invoke(jobj, "setSlangDictionary", args[["dictionary_path"]], args[["dictionary_delimiter"]],
                     read_as(x, args[["dictionary_read_as"]]), args[["dictionary_options"]])
  }

  new_nlp_normalizer(jobj)
}

#' @export
nlp_normalizer.ml_pipeline <- function(x, input_cols, output_col,
                 cleanup_patterns = NULL, lowercase = NULL, dictionary_path = NULL,
                 dictionary_delimiter = NULL, dictionary_read_as = "LINE_BY_LINE", dictionary_options = list("format" = "text"),
                 uid = random_string("normalizer_")) {

  stage <- nlp_normalizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    cleanup_patterns = cleanup_patterns,
    lowercase = lowercase,
    dictionary_path = dictionary_path,
    dictionary_delimiter = dictionary_delimiter,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_normalizer.tbl_spark <- function(x, input_cols, output_col,
                 cleanup_patterns = NULL, lowercase = NULL, dictionary_path = NULL,
                 dictionary_delimiter = NULL, dictionary_read_as = "LINE_BY_LINE", dictionary_options = list("format" = "text"),
                 uid = random_string("normalizer_")) {
  stage <- nlp_normalizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    cleanup_patterns = cleanup_patterns,
    lowercase = lowercase,
    dictionary_path = dictionary_path,
    dictionary_delimiter = dictionary_delimiter,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_normalizer <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["cleanup_patterns"]] <- cast_nullable_string_list(args[["cleanup_patterns"]])
  args[["lowercase"]] <- cast_nullable_logical(args[["lowercase"]])
  args[["dictionary_path"]] <- cast_nullable_string(args[["dictionary_path"]])
  args[["dictionary_delimiter"]] <- cast_nullable_string(args[["dictionary_delimiter"]])
  args[["dictionary_read_as"]] <- cast_choice(args[["dictionary_read_as"]], choices = c("LINE_BY_LINE", "SPARK_DATASET"))
  args[["dictionary_options"]] <- cast_nullable_string_list(args[["dictionary_options"]])
  args
}

new_nlp_normalizer <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_normalizer")
}

new_nlp_normalizer_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_normalizer_model")
}

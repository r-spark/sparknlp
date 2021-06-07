#' Spark NLP DocumentNormalizer
#'
#' Spark ML transformer which normalizes raw text from tagged text, e.g. 
#' scraped web pages or xml documents, from document type columns into Sentence. 
#' Removes all dirty characters from text following one or more input regex patterns.
#' Can apply not wanted character removal with a specific policy. Can apply lower case normalization.
#'
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#documentnormalizer-text-cleaning}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param action Action to perform applying regex patterns on text
#' @param encoding File encoding to apply on normalized documents (Default: "disable")
#' @param lower_case Whether to convert strings to lowercase (Default: false)
#' @param patterns Normalization regex patterns which match will be removed from document (Default: Array("<[^>]*>"))
#' @param policy RemovalPolicy to remove patterns from text with a given policy (Default: "pretty_all"). Possible values are "all", "pretty_all", "first", "pretty_first"
#' @param replacement Replacement string to apply when regexes match (Default: " ")
#' 
#' @export
nlp_document_normalizer <- function(x, input_cols, output_col,
                 action = NULL, encoding = NULL, lower_case = NULL, patterns = NULL, policy = NULL, replacement = NULL,
                 uid = random_string("document_normalizer_")) {
  UseMethod("nlp_document_normalizer")
}

#' @export
nlp_document_normalizer.spark_connection <- function(x, input_cols, output_col,
                 action = NULL, encoding = NULL, lower_case = NULL, patterns = NULL, policy = NULL, replacement = NULL,
                 uid = random_string("document_normalizer_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    action = action,
    encoding = encoding,
    lower_case = lower_case,
    patterns = patterns,
    policy = policy,
    replacement = replacement,
    uid = uid
  ) %>%
  validator_nlp_document_normalizer()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.DocumentNormalizer",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setAction", args[["action"]])  %>%
    sparklyr::jobj_set_param("setEncoding", args[["encoding"]])  %>%
    sparklyr::jobj_set_param("setLowercase", args[["lower_case"]])  %>%
    sparklyr::jobj_set_param("setPatterns", args[["patterns"]])  %>%
    sparklyr::jobj_set_param("setPolicy", args[["policy"]])  %>%
    sparklyr::jobj_set_param("setReplacement", args[["replacement"]]) 

  new_nlp_document_normalizer(jobj)
}

#' @export
nlp_document_normalizer.ml_pipeline <- function(x, input_cols, output_col,
                 action = NULL, encoding = NULL, lower_case = NULL, patterns = NULL, policy = NULL, replacement = NULL,
                 uid = random_string("document_normalizer_")) {

  stage <- nlp_document_normalizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    action = action,
    encoding = encoding,
    lower_case = lower_case,
    patterns = patterns,
    policy = policy,
    replacement = replacement,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_document_normalizer.tbl_spark <- function(x, input_cols, output_col,
                 action = NULL, encoding = NULL, lower_case = NULL, patterns = NULL, policy = NULL, replacement = NULL,
                 uid = random_string("document_normalizer_")) {
  stage <- nlp_document_normalizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    action = action,
    encoding = encoding,
    lower_case = lower_case,
    patterns = patterns,
    policy = policy,
    replacement = replacement,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_document_normalizer <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["action"]] <- cast_nullable_string(args[["action"]])
  args[["encoding"]] <- cast_nullable_string(args[["encoding"]])
  args[["lower_case"]] <- cast_nullable_logical(args[["lower_case"]])
  args[["patterns"]] <- cast_nullable_string_list(args[["patterns"]])
  args[["policy"]] <- cast_nullable_string(args[["policy"]])
  args[["replacement"]] <- cast_nullable_string(args[["replacement"]])
  args
}

nlp_float_params.nlp_document_normalizer <- function(x) {
  return(c())
}
new_nlp_document_normalizer <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_document_normalizer")
}

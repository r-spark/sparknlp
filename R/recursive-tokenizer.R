#' Spark NLP RecursiveTokenizer
#'
#' Spark ML model that tokenizes
#' See \url{https://nlp.johnsnowlabs.com/api/index#com.johnsnowlabs.nlp.annotators.RecursiveTokenizer}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param infixes strings that will be split when found at the middle of a token
#' @param prefixes strings that will be split when found at the beginning of a token
#' @param suffixes strings that will be split when found at the end of a token
#' @param white_list whitelist
#' 
#' @export
nlp_recursive_tokenizer <- function(x, input_cols, output_col,
                 infixes = NULL, prefixes = NULL, suffixes = NULL, white_list = NULL,
                 uid = random_string("recursive_tokenizer_")) {
  UseMethod("nlp_recursive_tokenizer")
}

#' @export
nlp_recursive_tokenizer.spark_connection <- function(x, input_cols, output_col,
                 infixes = NULL, prefixes = NULL, suffixes = NULL, white_list = NULL,
                 uid = random_string("recursive_tokenizer_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    infixes = infixes,
    prefixes = prefixes,
    suffixes = suffixes,
    white_list = white_list,
    uid = uid
  ) %>%
  validator_nlp_recursive_tokenizer()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.RecursiveTokenizer",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setInfixes", args[["infixes"]])  %>%
    sparklyr::jobj_set_param("setPrefixes", args[["prefixes"]])  %>%
    sparklyr::jobj_set_param("setSuffixes", args[["suffixes"]])  %>%
    sparklyr::jobj_set_param("setWhitelist", args[["white_list"]]) 

  new_nlp_recursive_tokenizer(jobj)
}

#' @export
nlp_recursive_tokenizer.ml_pipeline <- function(x, input_cols, output_col,
                 infixes = NULL, prefixes = NULL, suffixes = NULL, white_list = NULL,
                 uid = random_string("recursive_tokenizer_")) {

  stage <- nlp_recursive_tokenizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    infixes = infixes,
    prefixes = prefixes,
    suffixes = suffixes,
    white_list = white_list,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_recursive_tokenizer.tbl_spark <- function(x, input_cols, output_col,
                 infixes = NULL, prefixes = NULL, suffixes = NULL, white_list = NULL,
                 uid = random_string("recursive_tokenizer_")) {
  stage <- nlp_recursive_tokenizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    infixes = infixes,
    prefixes = prefixes,
    suffixes = suffixes,
    white_list = white_list,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_recursive_tokenizer <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["infixes"]] <- cast_nullable_string_list(args[["infixes"]])
  args[["prefixes"]] <- cast_nullable_string_list(args[["prefixes"]])
  args[["suffixes"]] <- cast_nullable_string_list(args[["suffixes"]])
  args[["white_list"]] <- cast_nullable_string_list(args[["white_list"]])
  args
}

new_nlp_recursive_tokenizer <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_recursive_tokenizer")
}

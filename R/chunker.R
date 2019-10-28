#' Spark NLP Chunker - Meaningful phrase matching
#'
#' Spark ML transformer that matches a pattern of part-of-speech tags in order to return meaningful phrases from document 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#chunker}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param regex_parsers the regular expression parsers to use for the chunking
#' @export
nlp_chunker <- function(x, input_cols, output_col,
                 regex_parsers = NULL,
                 uid = random_string("chunker_")) {
  UseMethod("nlp_chunker")
}

#' @export
nlp_chunker.spark_connection <- function(x, input_cols, output_col,
                 regex_parsers = NULL,
                 uid = random_string("chunker_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    regex_parsers = regex_parsers,
    uid = uid
  ) %>%
  validator_nlp_chunker()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.Chunker",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setRegexParsers", args[["regex_parsers"]]) 

  new_nlp_chunker(jobj)
}

#' @export
nlp_chunker.ml_pipeline <- function(x, input_cols, output_col,
                 regex_parsers = NULL,
                 uid = random_string("chunker_")) {

  stage <- nlp_chunker.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    regex_parsers = regex_parsers,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_chunker.tbl_spark <- function(x, input_cols, output_col,
                 regex_parsers = NULL,
                 uid = random_string("chunker_")) {
  stage <- nlp_chunker.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    regex_parsers = regex_parsers,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_chunker <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["regex_parsers"]] <- cast_nullable_string_list(args[["regex_parsers"]])
  args
}

new_nlp_chunker <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "chunker")
}

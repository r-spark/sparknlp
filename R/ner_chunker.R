#' Spark NLP NerChunker
#'
#' Spark ML transformer that extracts phrases that fit into a known pattern using the NER tags
#' See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_release_notes#1-nerchunker}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param regex_parsers A list of regex patterns to match chunks, for example: Array(“‹DT›?‹JJ›*‹NN›”)
#' 
#' @export
nlp_ner_chunker <- function(x, input_cols, output_col,
                 regex_parsers = NULL,
                 uid = random_string("ner_chunker_")) {
  UseMethod("nlp_ner_chunker")
}

#' @export
nlp_ner_chunker.spark_connection <- function(x, input_cols, output_col,
                 regex_parsers = NULL,
                 uid = random_string("ner_chunker_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    regex_parsers = regex_parsers,
    uid = uid
  ) %>%
  validator_nlp_ner_chunker()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.ner.NerChunker",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setRegexParsers", args[["regex_parsers"]]) 

  new_nlp_ner_chunker(jobj)
}

#' @export
nlp_ner_chunker.ml_pipeline <- function(x, input_cols, output_col,
                 regex_parsers = NULL,
                 uid = random_string("ner_chunker_")) {

  stage <- nlp_ner_chunker.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    regex_parsers = regex_parsers,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_ner_chunker.tbl_spark <- function(x, input_cols, output_col,
                 regex_parsers = NULL,
                 uid = random_string("ner_chunker_")) {
  stage <- nlp_ner_chunker.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    regex_parsers = regex_parsers,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_ner_chunker <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["regex_parsers"]] <- cast_nullable_string_list(args[["regex_parsers"]])
  args
}

nlp_float_params.nlp_ner_chunker <- function(x) {
  return(c())
}
new_nlp_ner_chunker <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_ner_chunker")
}

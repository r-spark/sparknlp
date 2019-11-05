#' Spark NLP Chunk2Doc
#'
#' Spark ML transformer that Converts a CHUNK type column back into DOCUMENT. Useful when trying to re-tokenize or do
#'  further analysis on a CHUNK result.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/transformers#chunk2doc}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' 
#' @export
nlp_chunk2doc <- function(x, input_cols, output_col,
                 uid = random_string("chunk2doc_")) {
  UseMethod("nlp_chunk2doc")
}

#' @export
nlp_chunk2doc.spark_connection <- function(x, input_cols, output_col,
                 uid = random_string("chunk2doc_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  ) %>%
  validator_nlp_chunk2doc()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.Chunk2Doc",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  )

  new_nlp_chunk2doc(jobj)
}

#' @export
nlp_chunk2doc.ml_pipeline <- function(x, input_cols, output_col,
                 uid = random_string("chunk2doc_")) {

  stage <- nlp_chunk2doc.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_chunk2doc.tbl_spark <- function(x, input_cols, output_col,
                 uid = random_string("chunk2doc_")) {
  stage <- nlp_chunk2doc.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_chunk2doc <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args
}

new_nlp_chunk2doc <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_chunk2doc")
}

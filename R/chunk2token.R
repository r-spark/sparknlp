#' Spark NLP Chunk2Token
#'
#' Spark ML transformer that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_annotators#chunk2token}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' 
#' @export
nlp_chunk2token <- function(x, input_cols, output_col,
                 uid = random_string("chunk2token_")) {
  UseMethod("nlp_chunk2token")
}

#' @export
nlp_chunk2token.spark_connection <- function(x, input_cols, output_col,
                 uid = random_string("chunk2token_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  ) %>%
  validator_nlp_chunk2token()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.Chunk2Token",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  )

  new_nlp_chunk2token(jobj)
}

#' @export
nlp_chunk2token.ml_pipeline <- function(x, input_cols, output_col,
                 uid = random_string("chunk2token_")) {

  stage <- nlp_chunk2token.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_chunk2token.tbl_spark <- function(x, input_cols, output_col,
                 uid = random_string("chunk2token_")) {
  stage <- nlp_chunk2token.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}

#' @import forge
validator_nlp_chunk2token <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args
}

nlp_float_params.nlp_chunk2token <- function(x) {
  return(c())
}
new_nlp_chunk2token <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_chunk2token")
}

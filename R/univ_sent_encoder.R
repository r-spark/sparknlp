#' Spark NLP UniversalSentenceEncoder
#'
#' Spark ML transformer that encodes text into high dimensional vectors that can be used for text classification, 
#' semantic similarity, clustering and other natural language tasks.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#universalsentenceencoder}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param dimension dimension to use for the embeddings
#' 
#' @export
nlp_univ_sent_encoder <- function(x, input_cols, output_col,
                 dimension = NULL,
                 uid = random_string("univ_sent_encoder_")) {
  UseMethod("nlp_univ_sent_encoder")
}

#' @export
nlp_univ_sent_encoder.spark_connection <- function(x, input_cols, output_col,
                 dimension = NULL,
                 uid = random_string("univ_sent_encoder_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    dimension = dimension,
    uid = uid
  ) %>%
  validator_nlp_univ_sent_encoder()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.embeddings.UniversalSentenceEncoder",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setDimension", args[["dimension"]]) 

  new_nlp_univ_sent_encoder(jobj)
}

#' @export
nlp_univ_sent_encoder.ml_pipeline <- function(x, input_cols, output_col,
                 dimension = NULL,
                 uid = random_string("univ_sent_encoder_")) {

  stage <- nlp_univ_sent_encoder.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    dimension = dimension,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_univ_sent_encoder.tbl_spark <- function(x, input_cols, output_col,
                 dimension = NULL,
                 uid = random_string("univ_sent_encoder_")) {
  stage <- nlp_univ_sent_encoder.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    dimension = dimension,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_univ_sent_encoder <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["dimension"]] <- cast_nullable_integer(args[["dimension"]])
  args
}

new_nlp_univ_sent_encoder <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_univ_sent_encoder")
}

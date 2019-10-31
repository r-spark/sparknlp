#' Spark NLP ChunkEmbeddings
#'
#' Spark ML transformer that utilizes WordEmbeddings or BertEmbeddings to generate chunk embeddings from either Chunker,
#' NGramGenerator, or NerConverter outputs.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#chunkembeddings}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param pooling_strategy Choose how you would like to aggregate Word Embeddings to Sentence Embeddings: AVERAGE or SUM
#' 
#' @export
nlp_chunk_embeddings <- function(x, input_cols, output_col,
                 pooling_strategy = NULL,
                 uid = random_string("chunk_embeddings_")) {
  UseMethod("nlp_chunk_embeddings")
}

#' @export
nlp_chunk_embeddings.spark_connection <- function(x, input_cols, output_col,
                 pooling_strategy = NULL,
                 uid = random_string("chunk_embeddings_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    pooling_strategy = pooling_strategy,
    uid = uid
  ) %>%
  validator_nlp_chunk_embeddings()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.embeddings.ChunkEmbeddings",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setPoolingStrategy", args[["pooling_strategy"]]) 

  new_nlp_chunk_embeddings(jobj)
}

#' @export
nlp_chunk_embeddings.ml_pipeline <- function(x, input_cols, output_col,
                 pooling_strategy = NULL,
                 uid = random_string("chunk_embeddings_")) {

  stage <- nlp_chunk_embeddings.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    pooling_strategy = pooling_strategy,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_chunk_embeddings.tbl_spark <- function(x, input_cols, output_col,
                 pooling_strategy = NULL,
                 uid = random_string("chunk_embeddings_")) {
  stage <- nlp_chunk_embeddings.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    pooling_strategy = pooling_strategy,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_chunk_embeddings <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["pooling_strategy"]] <- cast_choice(args[["pooling_strategy"]], choices = c("AVERAGE", "SUM"), allow_null = TRUE)
  args
}

new_nlp_chunk_embeddings <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_chunk_embeddings")
}

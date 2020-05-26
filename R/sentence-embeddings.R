#' Spark NLP SentenceEmbeddings
#'
#' Spark ML transformer that converts the results from WordEmbeddings or BertEmbeddings into sentence or document 
#' embeddings by either summing up or averaging all the word embeddings in a sentence or a document
#' (depending on the input_cols).
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#sentenceembeddings}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param pooling_strategy Choose how you would like to aggregate Word Embeddings to Sentence Embeddings: AVERAGE or SUM
#' @param storage_ref storage reference for the embeddings
#' 
#' @export
nlp_sentence_embeddings <- function(x, input_cols, output_col,
                 pooling_strategy = NULL, storage_ref = NULL,
                 uid = random_string("sentence_embeddings_")) {
  UseMethod("nlp_sentence_embeddings")
}

#' @export
nlp_sentence_embeddings.spark_connection <- function(x, input_cols, output_col,
                 pooling_strategy = NULL, storage_ref = NULL,
                 uid = random_string("sentence_embeddings_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    pooling_strategy = pooling_strategy,
    storage_ref = storage_ref,
    uid = uid
  ) %>%
  validator_nlp_sentence_embeddings()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.embeddings.SentenceEmbeddings",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setPoolingStrategy", args[["pooling_strategy"]]) %>% 
    sparklyr::jobj_set_param("setStorageRef", args[["storage_ref"]])

  new_nlp_sentence_embeddings(jobj)
}

#' @export
nlp_sentence_embeddings.ml_pipeline <- function(x, input_cols, output_col,
                 pooling_strategy = NULL, storage_ref = NULL,
                 uid = random_string("sentence_embeddings_")) {

  stage <- nlp_sentence_embeddings.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    pooling_strategy = pooling_strategy,
    storage_ref = storage_ref,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_sentence_embeddings.tbl_spark <- function(x, input_cols, output_col,
                 pooling_strategy = NULL, storage_ref = NULL,
                 uid = random_string("sentence_embeddings_")) {
  stage <- nlp_sentence_embeddings.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    pooling_strategy = pooling_strategy,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_sentence_embeddings <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["pooling_strategy"]] <- cast_choice(args[["pooling_strategy"]], choices = c("AVERAGE", "SUM"), allow_null = TRUE)
  args[["storage_ref"]] <- cast_nullable_string(args[["storage_ref"]])
  args
}

new_nlp_sentence_embeddings <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_sentence_embeddings")
}

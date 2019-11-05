#' Spark NLP EmbeddingsFinisher
#'
#' Spark ML transformer that is designed to deal with embedding annotators: WordEmbeddings, BertEmbeddings, 
#' SentenceEmbeddingd, and ChunkEmbeddings. By using EmbeddingsFinisher you can easily transform your embeddings 
#' into array of floats or Vectors which are compatible with Spark ML functions such as LDA, K-mean, Random Forest
#'  classifier or any other functions that require featureCol.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/transformers#embeddingsfinisher}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-outputs-params
#' @param clean_annotations Whether to remove and cleanup the rest of the annotators (columns)
#' @param output_as_vector  if enabled, it will output the embeddings as Vectors instead of arrays
#' 
#' @export
nlp_embeddings_finisher <- function(x, input_cols, output_cols,
                 clean_annotations = NULL, output_as_vector = NULL,
                 uid = random_string("embeddings_finisher_")) {
  UseMethod("nlp_embeddings_finisher")
}

#' @export
nlp_embeddings_finisher.spark_connection <- function(x, input_cols, output_cols,
                 clean_annotations = NULL, output_as_vector = NULL,
                 uid = random_string("embeddings_finisher_")) {
  args <- list(
    input_cols = input_cols,
    output_cols = output_cols,
    clean_annotations = clean_annotations,
    output_as_vector = output_as_vector,
    uid = uid
  ) %>%
  validator_nlp_embeddings_finisher()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.EmbeddingsFinisher",
    input_cols = args[["input_cols"]],
    output_cols = args[["output_cols"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setCleanAnnotations", args[["clean_annotations"]])  %>%
    sparklyr::jobj_set_param("setOutputAsVector", args[["output_as_vector"]]) 

  new_nlp_embeddings_finisher(jobj)
}

#' @export
nlp_embeddings_finisher.ml_pipeline <- function(x, input_cols, output_cols,
                 clean_annotations = NULL, output_as_vector = NULL,
                 uid = random_string("embeddings_finisher_")) {

  stage <- nlp_embeddings_finisher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_cols = output_cols,
    clean_annotations = clean_annotations,
    output_as_vector = output_as_vector,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_embeddings_finisher.tbl_spark <- function(x, input_cols, output_cols,
                 clean_annotations = NULL, output_as_vector = NULL,
                 uid = random_string("embeddings_finisher_")) {
  stage <- nlp_embeddings_finisher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_cols = output_cols,
    clean_annotations = clean_annotations,
    output_as_vector = output_as_vector,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_embeddings_finisher <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_cols"]] <- cast_string_list(args[["output_cols"]])
  args[["clean_annotations"]] <- cast_nullable_logical(args[["clean_annotations"]])
  args[["output_as_vector"]] <- cast_nullable_logical(args[["output_as_vector"]])
  args
}

new_nlp_embeddings_finisher <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_embeddings_finisher")
}

#' Spark NLP WordEmbeddings
#'
#' Spark ML estimator that maps tokens to vectors
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#word-embeddings}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param source_path word embeddings file
#' @param dimension number of word embeddings dimensions
#' @param include_embeddings whether or not to include word embeddings when saving this annotator to disk (single or within pipeline)
#' @param embeddings_ref whether to use annotators under the provided name. This means these embeddings will be lookup
#'  from the cache by the ref name. This allows multiple annotators to utilize same word embeddings by ref name.
#' @param embeddings_format format of word embeddings files. One of:
#' \itemize{
#' \item text -> this format is usually used by Glove
#' \item binary -> this format is usually used by Word2Vec
#' \item spark-nlp -> internal format for already serialized embeddings. Use this only when resaving embeddings with Spark NLP
#' }
#' @param case_sensitive whether to ignore case in tokens for embeddings matching
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a WordEmbeddings estimator.
#' When \code{x} is a \code{ml_pipeline} the pipeline with the WordEmbeddings added. When \code{x}
#' is a \code{tbl_spark} a transformed \code{tbl_spark}  (note that the Dataframe passed in must have the input_cols specified).
#' 
#' @export
nlp_word_embeddings <- function(x, input_cols, output_col,
                 source_path, dimension, include_embeddings = NULL, embeddings_ref = NULL, embeddings_format, case_sensitive = NULL,
                 uid = random_string("word_embeddings_")) {
  UseMethod("nlp_word_embeddings")
}

#' @export
nlp_word_embeddings.spark_connection <- function(x, input_cols, output_col,
                                                 source_path, dimension, include_embeddings = NULL, embeddings_ref = NULL, embeddings_format, case_sensitive = NULL,
                                                 uid = random_string("word_embeddings_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    source_path = source_path,
    dimension = dimension,
    include_embeddings = include_embeddings,
    embeddings_format = embeddings_format,
    embeddings_ref = embeddings_ref,
    case_sensitive = case_sensitive,
    uid = uid
  ) %>%
  validator_nlp_word_embeddings()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.embeddings.WordEmbeddings",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setSourcePath", args[["source_path"]])  %>%
    sparklyr::jobj_set_param("setDimension", args[["dimension"]])  %>%
    sparklyr::jobj_set_param("setEmbeddingsFormat", args[["embeddings_format"]])  %>%
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]]) %>%
    sparklyr::jobj_set_param("setIncludeEmbeddings", args[["include_embeddings"]]) %>%
    sparklyr::jobj_set_param("setEmbeddingsRef", args[["embeddings_ref"]])

  new_nlp_word_embeddings(jobj)
}

#' @export
nlp_word_embeddings.ml_pipeline <- function(x, input_cols, output_col,
                                            source_path, dimension, include_embeddings = NULL, embeddings_ref = NULL, embeddings_format, case_sensitive = NULL,
                                            uid = random_string("word_embeddings_"))  {

  stage <- nlp_word_embeddings.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    source_path = source_path,
    dimension = dimension,
    include_embeddings = include_embeddings,
    embeddings_format = embeddings_format,
    embeddings_ref = embeddings_ref,
    case_sensitive = case_sensitive,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_word_embeddings.tbl_spark <- function(x, input_cols, output_col,
                                          source_path, dimension, include_embeddings = NULL, embeddings_ref = NULL, embeddings_format, case_sensitive = NULL,
                                          uid = random_string("word_embeddings_"))  {
  stage <- nlp_word_embeddings.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    source_path = source_path,
    dimension = dimension,
    include_embeddings = include_embeddings,
    embeddings_format = embeddings_format,
    embeddings_ref = embeddings_ref,
    case_sensitive = case_sensitive,
    uid = uid
  )

  stage %>% sparklyr::ml_fit(x)
}

#' @import forge
validator_nlp_word_embeddings <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["source_path"]] <- cast_string(args[["source_path"]])
  args[["dimension"]] <- cast_integer(args[["dimension"]])
  args[["include_embeddings"]] <- cast_nullable_logical(args[["include_embeddings"]])
  args[["embeddings_format"]] <- cast_string(args[["embeddings_format"]])
  args[["embeddings_ref"]] <- cast_nullable_string(args[["embeddings_ref"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args
}

#' @import forge
validator_nlp_word_embeddings_pretrained <- function(args) {
  args[["input_cols"]] <- cast_nullable_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args
}

new_nlp_word_embeddings <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_word_embeddings")
}

#' Load pretrained word embeddings
#' 
#' Loads pretrained word embeddings into a Spark NLP annotator
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @export
nlp_word_embeddings_pretrained <- function(sc, input_cols = NULL, output_col,
                                      name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  ) %>%
    validator_nlp_word_embeddings_pretrained()
  
  model_class <- "com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])
  
  new_ml_transformer(model)
}


#' Spark NLP WordEmbeddings
#'
#' Spark ML estimator that maps tokens to vectors
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#word-embeddings}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param storage_ref binding to NerDLModel trained by that embeddings
#' @param dimension number of word embeddings dimensions
#' @param case_sensitive whether to ignore case in tokens for embeddings matching
#' @param lazy_annotator boolean for laziness
#' @param read_cache_size size for the read cache
#' @param write_buffer_size size for the write cache
#' @param include_storage whether or not to include word embeddings when saving this annotator to disk (single or within pipeline)
#' @param storage_path word embeddings file
#' @param storage_path_format format of word embeddings files. One of:
#' \itemize{
#' \item text -> this format is usually used by Glove
#' \item binary -> this format is usually used by Word2Vec
#' \item spark-nlp -> internal format for already serialized embeddings. Use this only when resaving embeddings with Spark NLP
#' }
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a WordEmbeddings estimator.
#' When \code{x} is a \code{ml_pipeline} the pipeline with the WordEmbeddings added. When \code{x}
#' is a \code{tbl_spark} a transformed \code{tbl_spark}  (note that the Dataframe passed in must have the input_cols specified).
#' 
#' @export
nlp_word_embeddings <- function(x, input_cols, output_col, storage_path, storage_path_format = "TEXT",
                                storage_ref = NULL, dimension, case_sensitive = NULL,
                                lazy_annotator = NULL, read_cache_size = NULL, write_buffer_size = NULL,
                                include_storage = FALSE, uid = random_string("word_embeddings_")) {
  UseMethod("nlp_word_embeddings")
}

#' @export
nlp_word_embeddings.spark_connection <- function(x, input_cols, output_col, storage_path, storage_path_format = "TEXT",
                                                 storage_ref = NULL, dimension, case_sensitive = NULL,
                                                 lazy_annotator = NULL, read_cache_size = NULL, write_buffer_size = NULL,
                                                 include_storage = FALSE, uid = random_string("word_embeddings_")) {

  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    storage_path = storage_path,
    storage_path_format = storage_path_format,
    storage_ref = storage_ref,
    dimension = dimension,
    case_sensitive = case_sensitive,
    lazy_annotator = lazy_annotator,
    read_cache_size = read_cache_size,
    write_buffer_size = write_buffer_size,
    include_storage = include_storage,
    uid = uid
  ) %>%
  validator_nlp_word_embeddings()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.embeddings.WordEmbeddings",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setStorageRef", args[["storage_ref"]]) %>% 
    sparklyr::jobj_set_param("setDimension", args[["dimension"]])  %>%
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]]) %>%
    sparklyr::jobj_set_param("setLazyAnnotator", args[["lazy_annotator"]]) %>%
    sparklyr::jobj_set_param("setReadCacheSize", args[["read_cache_size"]]) %>%
    sparklyr::jobj_set_param("setWriteBufferSize", args[["write_buffer_size"]]) %>%
    sparklyr::jobj_set_param("setIncludeStorage", args[["include_storage"]])
    
  jobj <- invoke_static(x, "sparknlp.Utils", "setStoragePath", jobj, args[["storage_path"]], args[["storage_path_format"]])

  new_nlp_word_embeddings(jobj)
}

#' @export
nlp_word_embeddings.ml_pipeline <- function(x, input_cols, output_col, storage_path, storage_path_format = "TEXT",
                                            storage_ref = NULL, dimension, case_sensitive = NULL,
                                            lazy_annotator = NULL, read_cache_size = NULL, write_buffer_size = NULL,
                                            include_storage = NULL, uid = random_string("word_embeddings_"))  {

  stage <- nlp_word_embeddings.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    storage_path = storage_path,
    storage_path_format = storage_path_format,
    storage_ref = storage_ref,
    dimension = dimension,
    case_sensitive = case_sensitive,
    lazy_annotator = lazy_annotator,
    read_cache_size = read_cache_size,
    write_buffer_size = write_buffer_size,
    include_storage = include_storage,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_word_embeddings.tbl_spark <- function(x, input_cols, output_col, storage_path, storage_path_format = "TEXT",
                                          storage_ref = NULL, dimension, case_sensitive = NULL,
                                          lazy_annotator = NULL, read_cache_size = NULL, write_buffer_size = NULL,
                                          include_storage = NULL, uid = random_string("word_embeddings_"))  {
  stage <- nlp_word_embeddings.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    storage_path = storage_path,
    storage_path_format = storage_path_format,
    storage_ref = storage_ref,
    dimension = dimension,
    case_sensitive = case_sensitive,
    lazy_annotator = lazy_annotator,
    read_cache_size = read_cache_size,
    write_buffer_size = write_buffer_size,
    include_storage = include_storage,
    uid = uid
  )

  stage %>% sparklyr::ml_fit(x)
}

#' @import forge
validator_nlp_word_embeddings <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["storage_path"]] <- cast_string(args[["storage_path"]])
  args[["storage_path_format"]] <- cast_choice(args[["storage_path_format"]], c("TEXT", "SPARK", "BINARY"))
  args[["storage_ref"]] <- cast_nullable_string(args[["storage_ref"]])
  args[["dimension"]] <- cast_integer(args[["dimension"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["lazy_annotator"]] <- cast_nullable_logical(args[["lazy_annotator"]])
  args[["read_cache_size"]] <- cast_nullable_integer(args[["read_cache_size"]])
  args[["write_buffer_size"]] <- cast_nullable_integer(args[["write_buffer_size"]])
  args[["include_storage"]] <- cast_nullable_logical(args[["include_storage"]])
  args
}

#' @import forge
validator_nlp_word_embeddings_pretrained <- function(args) {
  args[["input_cols"]] <- cast_nullable_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args
}

new_nlp_word_embeddings <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_word_embeddings")
}

new_nlp_word_embeddings_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_word_embeddings_model")
}

#' Load pretrained word embeddings
#' 
#' Loads pretrained word embeddings into a Spark NLP annotator
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param case_sensitive whether to treat the words as case sensitive
#' @export
nlp_word_embeddings_pretrained <- function(sc, input_cols = NULL, output_col,
                                      name = NULL, lang = NULL, remote_loc = NULL, case_sensitive = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    case_sensitive = case_sensitive
  ) %>%
    validator_nlp_word_embeddings_pretrained()
  
  model_class <- "com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])
  
  if (!is.null(args[["case_sensitive"]])) {
    sparklyr::jobj_set_param(spark_jobj(model), "setCaseSensitive", args[["case_sensitive"]])
  }
  
  new_nlp_word_embeddings_model(model)
}

#' Create a Spark NLP WordEmbeddingsModel
#' 
#' This function creates a WordEmbeddingsModel which uses the provided embeddings_ref.
#' 
#' @param Spark connection
#' @template roxlate-inputs-output-params
#' @param embeddings_ref the reference name for the embeddings cache that the model will use
#' @param dimension number of word embeddings dimensions
#' @param uid unique identifier for this instance
#' 
#' @return a Spark transformer WordEmbeddingsModel
#' 
#' @export
#' @import forge
nlp_word_embeddings_model <- function(sc, input_cols, output_col, storage_ref = NULL, dimension, case_sensitive = NULL,
                                      include_storage = NULL, lazy_annotator = NULL, read_cache_size = NULL, include_embeddings = NULL,
                                      uid = random_string("word_embeddings_")) {
  args <- list(
    input_cols = cast_string_list(input_cols),
    output_col = cast_string(output_col),
    case_sensitive = cast_nullable_string(case_sensitive),
    dimension = cast_integer(dimension),
    include_storage = cast_nullable_logical(include_storage),
    lazy_annotator = cast_nullable_logical(lazy_annotator),
    read_cache_size = cast_nullable_integer(read_cache_size),
    storage_ref = cast_nullable_string(storage_ref),
    uid = cast_string(uid)
  )
  
  jobj <- sparklyr::spark_pipeline_stage(sc, 
                                         "com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel", 
                                         uid) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>%
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>%
    sparklyr::jobj_set_param("setStorageRef", args[["storage_ref"]]) %>% 
    sparklyr::jobj_set_param("setDimension", args[["dimension"]])  %>%
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]]) %>%
    sparklyr::jobj_set_param("setLazyAnnotator", args[["lazy_annotator"]]) %>%
    sparklyr::jobj_set_param("setReadCacheSize", args[["read_cache_size"]]) %>%
    sparklyr::jobj_set_param("setIncludeStorage", args[["include_storage"]])

  new_nlp_word_embeddings_model(jobj)

}

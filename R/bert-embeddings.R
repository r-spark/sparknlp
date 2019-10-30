#' Spark NLP BertEmbeddings
#'
#' Spark ML transformer that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#bert-embeddings}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param batch_size 
#' @param case_sensitive 
#' @param config_proto_bytes 
#' @param dimension 
#' @param max_sentence_length 
#' @param model_if_not_set 
#' @param pooling_layer 
#' @param vocabulary 
#' 
#' @export
#' nlp_bert_embeddings <- function(x, input_cols, output_col,
#'                  batch_size = NULL, case_sensitive = NULL, config_proto_bytes = NULL, dimension = NULL, max_sentence_length = NULL, pooling_layer = NULL, vocabulary = NULL,
#'                  uid = random_string("bert_embeddings_")) {
#'   UseMethod("nlp_bert_embeddings")
#' }
#' 
#' #' @export
#' nlp_bert_embeddings.spark_connection <- function(x, input_cols, output_col,
#'                  batch_size = NULL, case_sensitive = NULL, config_proto_bytes = NULL, dimension = NULL, max_sentence_length = NULL, pooling_layer = NULL, vocabulary = NULL,
#'                  uid = random_string("bert_embeddings_")) {
#'   args <- list(
#'     input_cols = input_cols,
#'     output_col = output_col,
#'     batch_size = batch_size,
#'     case_sensitive = case_sensitive,
#'     config_proto_bytes = config_proto_bytes,
#'     dimension = dimension,
#'     max_sentence_length = max_sentence_length,
#'     pooling_layer = pooling_layer,
#'     vocabulary = vocabulary,
#'     uid = uid
#'   ) %>%
#'   validator_nlp_bert_embeddings()
#'   
#'   if (!is.null(args[["vocabulary"]])) {
#'     args[["vocabulary"]] <- list2env(args[["vocabulary"]])
#'   }
#' 
#'   jobj <- sparklyr::spark_pipeline_stage(
#'     x, "com.johnsnowlabs.nlp.embeddings.BertEmbeddings",
#'     input_cols = args[["input_cols"]],
#'     output_col = args[["output_col"]],
#'     uid = args[["uid"]]
#'   ) %>%
#'     sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]])  %>%
#'     sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]])  %>%
#'     sparklyr::jobj_set_param("setConfigProtoBytes", args[["config_proto_bytes"]])  %>%
#'     sparklyr::jobj_set_param("setDimension", args[["dimension"]])  %>%
#'     sparklyr::jobj_set_param("setMaxSentenceLength", args[["max_sentence_length"]])  %>%
#'     sparklyr::jobj_set_param("setModelIfNotSet", args[["model_if_not_set"]])  %>%
#'     sparklyr::jobj_set_param("setPoolingLayer", args[["pooling_layer"]])  %>%
#'     sparklyr::jobj_set_param("setVocabulary", args[["vocabulary"]]) 
#' 
#'   new_nlp_bert_embeddings(jobj)
#' }
#' 
#' #' @export
#' nlp_bert_embeddings.ml_pipeline <- function(x, input_cols, output_col,
#'                  batch_size = NULL, case_sensitive = NULL, config_proto_bytes = NULL, dimension = NULL, max_sentence_length = NULL, pooling_layer = NULL, vocabulary = NULL,
#'                  uid = random_string("bert_embeddings_")) {
#' 
#'   stage <- nlp_bert_embeddings.spark_connection(
#'     x = sparklyr::spark_connection(x),
#'     input_cols = input_cols,
#'     output_col = output_col,
#'     batch_size = batch_size,
#'     case_sensitive = case_sensitive,
#'     config_proto_bytes = config_proto_bytes,
#'     dimension = dimension,
#'     max_sentence_length = max_sentence_length,
#'     pooling_layer = pooling_layer,
#'     vocabulary = vocabulary,
#'     uid = uid
#'   )
#' 
#'   sparklyr::ml_add_stage(x, stage)
#' }
#' 
#' #' @export
#' nlp_bert_embeddings.tbl_spark <- function(x, input_cols, output_col,
#'                  batch_size = NULL, case_sensitive = NULL, config_proto_bytes = NULL, dimension = NULL, max_sentence_length = NULL, pooling_layer = NULL, vocabulary = NULL,
#'                  uid = random_string("bert_embeddings_")) {
#'   stage <- nlp_bert_embeddings.spark_connection(
#'     x = sparklyr::spark_connection(x),
#'     input_cols = input_cols,
#'     output_col = output_col,
#'     batch_size = batch_size,
#'     case_sensitive = case_sensitive,
#'     config_proto_bytes = config_proto_bytes,
#'     dimension = dimension,
#'     max_sentence_length = max_sentence_length,
#'     pooling_layer = pooling_layer,
#'     vocabulary = vocabulary,
#'     uid = uid
#'   )
#' 
#'   stage %>% sparklyr::ml_transform(x)
#' }

#' Load a pretrained Spark NLP model
#' 
#' Create a pretrained Spark NLP \code{BertEmbeddings} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @export
nlp_bert_embeddings_pretrained <- function(sc, input_cols, output_col,
                                      name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  ) %>%
    validator_nlp_bert_embeddings()
  
  model_class <- "com.johnsnowlabs.nlp.embeddings.BertEmbeddings"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])
  
  new_ml_transformer(model)
}

#' @import forge
validator_nlp_bert_embeddings <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["config_proto_bytes"]] <- cast_nullable_integer_list(args[["config_proto_bytes"]])
  args[["dimension"]] <- cast_nullable_integer(args[["dimension"]])
  args[["max_sentence_length"]] <- cast_nullable_integer(args[["max_sentence_length"]])
  args[["pooling_layer"]] <- cast_nullable_integer(args[["pooling_layer"]])
  args
}

new_nlp_bert_embeddings <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_bert_embeddings")
}


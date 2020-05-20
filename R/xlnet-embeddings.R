#' Load a pretrained Spark NLP XlnetEmbeddings model
#' 
#' Create a pretrained Spark NLP \code{XlnetEmbeddings} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param case_sensitive whether to treat the tokens as case insensitive when looking up their embedding
#' @param batch_size batch size
#' @param dimension the embedding dimension
#' @param lazy_annotator use as a lazy annotator or not
#' @param max_sentence_length set the maximum sentence length
#' @param storage_ref storage reference name
#' @export
nlp_xlnet_embeddings_pretrained <- function(sc, input_cols, output_col, case_sensitive = NULL,
                                           batch_size = NULL, dimension = NULL, 
                                           lazy_annotator = NULL, max_sentence_length = NULL, storage_ref = NULL,
                                      name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    case_sensitive = case_sensitive,
    batch_size = batch_size,
    dimension = dimension,
    lazy_annotator = lazy_annotator,
    max_sentence_length = max_sentence_length,
    storage_ref = storage_ref
  ) %>%
    validator_nlp_xlnet_embeddings()
  
  model_class <- "com.johnsnowlabs.nlp.embeddings.XlnetEmbeddings"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>% 
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]]) %>% 
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]]) %>% 
    sparklyr::jobj_set_param("setDimension", args[["dimension"]]) %>% 
    sparklyr::jobj_set_param("setLazyAnnotator", args[["lazy_annotator"]]) %>% 
    sparklyr::jobj_set_param("setMaxSentenceLength", args[["max_sentence_length"]]) %>% 
    sparklyr::jobj_set_param("setStorageRef", args[["storage_ref"]])
  
  new_ml_transformer(model)
}

#' @import forge
validator_nlp_xlnet_embeddings <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["dimension"]] <- cast_nullable_integer(args[["dimension"]])
  args[["lazy_annotator"]] <- cast_nullable_logical(args[["lazy_annotator"]])
  args[["max_sentence_length"]] <- cast_nullable_integer(args[["max_sentence_length"]])
  args[["storage_ref"]] <- cast_nullable_string(args[["storage_ref"]])
  args
}

new_nlp_xlnet_embeddings <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_xlnet_embeddings")
}


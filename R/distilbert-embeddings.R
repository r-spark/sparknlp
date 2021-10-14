#' Load a pretrained Spark NLP DistilBertEmbeddings model
#'
#' DistilBERT is a small, fast, cheap and light Transformer model trained by distilling BERT base. 
#' It has 40% less parameters than bert-base-uncased, runs 60% faster while preserving over 95% of 
#' BERT's performances as measured on the GLUE language understanding benchmark.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/transformers#distilbertembeddings}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param batch_size Size of every batch (Default depends on model).
#' @param case_sensitive Whether to ignore case in index lookups (Default depends on model)
#' @param dimension Number of embedding dimensions (Default depends on model)
#' @param max_sentence_length Max sentence length to process (Default: 128)
#' @param storage_ref Unique identifier for storage (Default: this.uid)
#' 
#' @export
nlp_distilbert_embeddings_pretrained <- function(sc, input_cols, output_col,
                                                 batch_size = NULL, case_sensitive = NULL, dimension = NULL,  
                                                 max_sentence_length = NULL, storage_ref = NULL,
                                                 name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    case_sensitive = case_sensitive,
    dimension = dimension,
    max_sentence_length = max_sentence_length,
    storage_ref = storage_ref
  ) %>%
    validator_nlp_distilbert_embeddings()
  
  model_class <- "com.johnsnowlabs.nlp.embeddings.DistilBertEmbeddings"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>% 
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]]) %>% 
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]]) %>% 
    sparklyr::jobj_set_param("setDimension", args[["dimension"]]) %>% 
    sparklyr::jobj_set_param("setMaxSentenceLength", args[["max_sentence_length"]]) %>% 
    sparklyr::jobj_set_param("setStorageRef", args[["storage_ref"]])
  
  new_ml_transformer(model)
}

#' @import forge
validator_nlp_distilbert_embeddings <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["dimension"]] <- cast_nullable_integer(args[["dimension"]])
  args[["max_sentence_length"]] <- cast_nullable_integer(args[["max_sentence_length"]])
  args[["storage_ref"]] <- cast_nullable_string(args[["storage_ref"]])
  args
}

new_nlp_distilbert_embeddings <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_distilbert_embeddings")
}

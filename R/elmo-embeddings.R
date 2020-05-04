#' Load a pretrained Spark NLP ElmoEmbeddings model
#' 
#' Create a pretrained Spark NLP \code{ElmoEmbeddings} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param case_sensitive whether to treat the tokens as case insensitive when looking up their embedding
#' @param batch_size batch size
#' @param dimension the embedding dimension
#' @param pooling_layer word_emb, lstm_outputs1, lstm_outputs2 or elmo
#' @export
nlp_elmo_embeddings_pretrained <- function(sc, input_cols, output_col, case_sensitive = NULL,
                                           batch_size = NULL, dimension = NULL, pooling_layer = NULL,
                                      name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    case_sensitive = case_sensitive,
    batch_size = batch_size,
    dimension = dimension,
    pooling_layer = pooling_layer
  ) %>%
    validator_nlp_elmo_embeddings()
  
  model_class <- "com.johnsnowlabs.nlp.embeddings.ElmoEmbeddings"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>% 
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]]) %>% 
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]]) %>% 
    sparklyr::jobj_set_param("setDimension", args[["dimension"]]) %>% 
    sparklyr::jobj_set_param("setPoolingLayer", args[["pooling_layer"]])
  
  new_ml_transformer(model)
}

#' @import forge
validator_nlp_elmo_embeddings <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["dimension"]] <- cast_nullable_integer(args[["dimension"]])
  args[["pooling_layer"]] <- cast_nullable_string(args[["pooling_layer"]])
  args
}

new_nlp_elmo_embeddings <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_elmo_embeddings")
}


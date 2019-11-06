#' Load a pretrained Spark NLP BertEmbeddings model
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
  args
}

new_nlp_bert_embeddings <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_bert_embeddings")
}


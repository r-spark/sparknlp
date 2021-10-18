#' Spark NLP RoBertaForTokenClassification
#'
#' RoBertaForTokenClassification can load RoBERTa Models with a token classification head on top
#' (a linear layer on top of the hidden-states output) e.g. for Named-Entity-Recognition (NER) tasks.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/transformers#xlnetfortokenclassification}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param batch_size Size of every batch (Default depends on model).
#' @param case_sensitive Whether to ignore case in index lookups (Default depends on model)
#' @param max_sentence_length Max sentence length to process (Default: 128)
#' 
#' @export
nlp_roberta_token_classification_pretrained <- function(sc, input_cols, output_col,
                                                           batch_size = NULL, case_sensitive = NULL, 
                                                           max_sentence_length = NULL,
                                                           name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    case_sensitive = case_sensitive,
    max_sentence_length = max_sentence_length
  ) %>%
    validator_nlp_roberta_token_classification()
  
  model_class <- "com.johnsnowlabs.nlp.annotators.classifier.dl.RoBertaForTokenClassification"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>% 
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]]) %>% 
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]]) %>% 
    sparklyr::jobj_set_param("setMaxSentenceLength", args[["max_sentence_length"]])
  
  new_ml_transformer(model)
}

#' @import forge
validator_nlp_roberta_token_classification <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["max_sentence_length"]] <- cast_nullable_integer(args[["max_sentence_length"]])
  args
}

new_nlp_roberta_for_token_classification <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_roberta_for_token_classification")
}


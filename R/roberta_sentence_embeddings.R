#' Load a pretrained Spark NLP RoBertaSentenceEmbeddings model
#' 
#' Create a pretrained Spark NLP \code{RoBertaSentenceEmbeddings} model.
#' Sentence-level embeddings using RoBERTa. The RoBERTa model was proposed in 
#' RoBERTa: A Robustly Optimized BERT Pretraining Approach by Yinhan Liu, Myle Ott, 
#' Naman Goyal, Jingfei Du, Mandar Joshi, Danqi Chen, Omer Levy, Mike Lewis, 
#' Luke Zettlemoyer, Veselin Stoyanov. It is based on Google's BERT model 
#' released in 2018.
#' 
#' It builds on BERT and modifies key hyperparameters, removing the next-sentence pretraining objective and training with much larger mini-batches and learning rates.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#robertabertsentenceembeddings}
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param batch_size batch size
#' @param case_sensitive whether to lowercase tokens or not
#' @param dimension defines the output layer of BERT when calculating embeddings
#' @param max_sentence_length max sentence length to process
#' 
#' @export
nlp_roberta_sentence_embeddings_pretrained <- function(sc, input_cols, output_col, case_sensitive = NULL,
                                              batch_size = NULL, dimension = NULL, 
                                              max_sentence_length = NULL, 
                                              name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    case_sensitive = case_sensitive,
    batch_size = batch_size,
    dimension = dimension,
    max_sentence_length = max_sentence_length
  ) %>%
    validator_nlp_roberta_sentence_embeddings()
  
  model_class <- "com.johnsnowlabs.nlp.embeddings.RoBertaSentenceEmbeddings"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>% 
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]]) %>% 
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]]) %>% 
    sparklyr::jobj_set_param("setDimension", args[["dimension"]]) %>% 
    sparklyr::jobj_set_param("setMaxSentenceLength", args[["max_sentence_length"]])
  
  new_nlp_roberta_sentence_embeddings(model)
}

#' @import forge
validator_nlp_roberta_sentence_embeddings <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["dimension"]] <- cast_nullable_integer(args[["dimension"]])
  args[["max_sentence_length"]] <- cast_nullable_integer(args[["max_sentence_length"]])
  args
}

new_nlp_roberta_sentence_embeddings <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_roberta_sentence_embeddings")
}

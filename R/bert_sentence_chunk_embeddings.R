#' Load a pretrained Spark NLP BertSentenceChunkEmbeddings model
#' 
#' Create a pretrained Spark NLP \code{BertSentenceChunkEmbeddings} model.
#' BERT Sentence embeddings for chunk annotations which take into account the context 
#' of the sentence the chunk appeared in. This is an extension of BertSentenceEmbeddings
#' which combines the embedding of a chunk with the embedding of the surrounding sentence.
#' For each input chunk annotation, it finds the corresponding sentence, computes the BERT
#' sentence embedding of both the chunk and the sentence and averages them. The resulting
#' embeddings are useful in cases, in which one needs a numerical representation of a text
#' chunk which is sensitive to the context it appears in.
#' 
#' This model is a subclass of BertSentenceEmbeddings and shares all parameters with it. It can
#' load any pretrained BertSentenceEmbeddings model. 
#' See \url{https://nlp.johnsnowlabs.com/licensed/api/com/johnsnowlabs/nlp/annotators/embeddings/BertSentenceChunkEmbeddings.html}
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @param batch_size batch size
#' @param case_sensitive whether to lowercase tokens or not
#' @param dimension defines the output layer of BERT when calculating embeddings
#' @param max_sentence_length max sentence length to process
#' 
#' @export
nlp_bert_sentence_chunk_embeddings_pretrained <- function(sc, input_cols, output_col, case_sensitive = NULL,
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
    validator_nlp_bert_sentence_chunk_embeddings()
  
  model_class <- "com.johnsnowlabs.nlp.annotators.embeddings.BertSentenceChunkEmbeddings"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]]) %>% 
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]]) %>% 
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]]) %>% 
    sparklyr::jobj_set_param("setDimension", args[["dimension"]]) %>% 
    sparklyr::jobj_set_param("setMaxSentenceLength", args[["max_sentence_length"]])
  
  new_nlp_bert_sentence_chunk_embeddings(model)
}

#' @import forge
validator_nlp_bert_sentence_chunk_embeddings <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["dimension"]] <- cast_nullable_integer(args[["dimension"]])
  args[["max_sentence_length"]] <- cast_nullable_integer(args[["max_sentence_length"]])
  args
}

new_nlp_bert_sentence_chunk_embeddings <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_bert_sentence_chunk_embeddings")
}

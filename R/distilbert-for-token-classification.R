#' Spark NLP DistilBertForTokenClassification
#'
#' Spark ML transformer that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/transformers#distilbertfortokenclassification}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param batch_size Size of every batch (Default depends on model).
#' @param case_sensitive Whether to ignore case in index lookups (Default depends on model)
#' @param max_sentence_length Max sentence length to process (Default: 128)
#' 
#' @export
nlp_distilbert_for_token_classification <- function(x, input_cols, output_col,
                 batch_size = NULL, case_sensitive = NULL, max_sentence_length = NULL,
                 uid = random_string("distilbert_for_token_classification_")) {
  UseMethod("nlp_distilbert_for_token_classification")
}

#' @export
nlp_distilbert_for_token_classification.spark_connection <- function(x, input_cols, output_col,
                 batch_size = NULL, case_sensitive = NULL, max_sentence_length = NULL,
                 uid = random_string("distilbert_for_token_classification_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    case_sensitive = case_sensitive,
    max_sentence_length = max_sentence_length,
    uid = uid
  ) %>%
  validator_nlp_distilbert_for_token_classification()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.classifier.dl.DistilBertForTokenClassification",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setBatchSize", args[["batch_size"]])  %>%
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]])  %>%
    sparklyr::jobj_set_param("setMaxSentenceLength", args[["max_sentence_length"]]) 

  new_nlp_distilbert_for_token_classification(jobj)
}

#' @export
nlp_distilbert_for_token_classification.ml_pipeline <- function(x, input_cols, output_col,
                 batch_size = NULL, case_sensitive = NULL, max_sentence_length = NULL,
                 uid = random_string("distilbert_for_token_classification_")) {

  stage <- nlp_distilbert_for_token_classification.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    case_sensitive = case_sensitive,
    max_sentence_length = max_sentence_length,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_distilbert_for_token_classification.tbl_spark <- function(x, input_cols, output_col,
                 batch_size = NULL, case_sensitive = NULL, max_sentence_length = NULL,
                 uid = random_string("distilbert_for_token_classification_")) {
  stage <- nlp_distilbert_for_token_classification.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    batch_size = batch_size,
    case_sensitive = case_sensitive,
    max_sentence_length = max_sentence_length,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_distilbert_for_token_classification <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["batch_size"]] <- cast_nullable_integer(args[["batch_size"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["max_sentence_length"]] <- cast_nullable_integer(args[["max_sentence_length"]])
  args
}

nlp_float_params.nlp_distilbert_for_token_classification <- function(x) {
  return(c())
}
new_nlp_distilbert_for_token_classification <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_distilbert_for_token_classification")
}

#' Spark NLP Stemmer
#'
#' Spark ML transformer that returns hard-stems out of words with the objective of retrieving the meaningful part of the word
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#stemmer}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param language language to use
#' 
#' @export
nlp_stemmer <- function(x, input_cols, output_col,
                 language = NULL,
                 uid = random_string("stemmer_")) {
  UseMethod("nlp_stemmer")
}

#' @export
nlp_stemmer.spark_connection <- function(x, input_cols, output_col,
                 language = NULL,
                 uid = random_string("stemmer_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    language = language,
    uid = uid
  ) %>%
  validator_nlp_stemmer()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.Stemmer",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setLanguage", args[["language"]]) 

  new_nlp_stemmer(jobj)
}

#' @export
nlp_stemmer.ml_pipeline <- function(x, input_cols, output_col,
                 language = NULL,
                 uid = random_string("stemmer_")) {

  stage <- nlp_stemmer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    language = language,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_stemmer.tbl_spark <- function(x, input_cols, output_col,
                 language = NULL,
                 uid = random_string("stemmer_")) {
  stage <- nlp_stemmer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    language = language,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_stemmer <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["language"]] <- cast_nullable_string(args[["language"]])
  args
}

new_nlp_stemmer <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_stemmer")
}

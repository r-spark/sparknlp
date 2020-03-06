#' Spark NLP Lemmatizer
#'
#' Spark ML estimator that retrieves lemmas out of words with the objective of returning a base dictionary word
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#lemmatizer}
#' 
#' @template roxlate-nlp-ml-algo
#' @template roxlate-inputs-output-params
#' @param dictionary_path Path to lemma dictionary, in lemma vs possible words format.
#' @param dictionary_key_delimiter key delimiter in the dictionary file
#' @param dictionary_value_delimiter value delimiter in the dictionary file
#' @param dictionary_read_as readAs TEXT or SPARK_DATASET 
#' @param dictionary_options options passed to the spark reader if read_as is SPARK_DATASET
#' 
#' 
#' @export
nlp_lemmatizer <- function(x, input_cols, output_col,
                 dictionary_path = NULL, dictionary_key_delimiter = "->", dictionary_value_delimiter = "\t", 
                 dictionary_read_as = "TEXT", dictionary_options = list(format = "text"),
                 uid = random_string("lemmatizer_")) {
  UseMethod("nlp_lemmatizer")
}

#' @export
nlp_lemmatizer.spark_connection <- function(x, input_cols, output_col,
                 dictionary_path = NULL, dictionary_key_delimiter = "->", dictionary_value_delimiter = "\t", 
                 dictionary_read_as = "TEXT", dictionary_options = list(format = "text"),
                 uid = random_string("lemmatizer_")) {
  
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    dictionary_path = dictionary_path,
    dictionary_key_delimiter = dictionary_key_delimiter,
    dictionary_value_delimiter = dictionary_value_delimiter,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    uid = uid
  ) %>%
  validator_nlp_lemmatizer()
  
  if (!is.null(args[["dictionary_options"]])) {
    args[["dictionary_options"]] <- list2env(args[["dictionary_options"]])
  }

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.Lemmatizer",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  )

  if (!is.null(args[["dictionary_path"]])) {
    sparklyr::invoke(jobj, "setDictionary", args[["dictionary_path"]], args[["dictionary_key_delimiter"]],
                     args[["dictionary_value_delimiter"]], read_as(x, args[["dictionary_read_as"]]), args[["dictionary_options"]])
  }

  new_nlp_lemmatizer(jobj)
}

#' @export
nlp_lemmatizer.ml_pipeline <- function(x, input_cols, output_col,
                 dictionary_path = NULL, dictionary_key_delimiter = "->", dictionary_value_delimiter = "\t", 
                 dictionary_read_as = "TEXT", dictionary_options = list(format = "text"),
                 uid = random_string("lemmatizer_")) {

  stage <- nlp_lemmatizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    dictionary_path = dictionary_path,
    dictionary_key_delimiter = dictionary_key_delimiter,
    dictionary_value_delimiter = dictionary_value_delimiter,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_lemmatizer.tbl_spark <- function(x, input_cols, output_col,
                 dictionary_path = NULL, dictionary_key_delimiter = "->", dictionary_value_delimiter = "\t", 
                 dictionary_read_as = "TEXT", dictionary_options = list(format = "text"),
                 uid = random_string("lemmatizer_")) {
  stage <- nlp_lemmatizer.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    dictionary_path = dictionary_path,
    dictionary_key_delimiter = dictionary_key_delimiter,
    dictionary_value_delimiter = dictionary_value_delimiter,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    uid = uid
  )

  stage %>% sparklyr::ml_fit(x)
}


#' Load a pretrained Spark NLP model
#' 
#' Create a pretrained Spark NLP \code{LemmatizerModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @export
nlp_lemmatizer_pretrained <- function(sc, input_cols, output_col,
                                      name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  ) %>%
    validator_nlp_lemmatizer()
  
  model_class <- "com.johnsnowlabs.nlp.annotators.LemmatizerModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])
  
  new_ml_transformer(model)
}

#' @import forge
validator_nlp_lemmatizer <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["dictionary_path"]] <- cast_nullable_string(args[["dictionary_path"]])
  args[["dictionary_key_delimiter"]] <- cast_nullable_string(args[["dictionary_key_delimiter"]])
  args[["dictionary_value_delimiter"]] <- cast_nullable_string(args[["dictionary_value_delimiter"]])
  args
}

new_nlp_lemmatizer <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_lemmatizer")
}

#' Spark NLP SymmetricDeleteApproach
#'
#' Spark ML estimator that is a spell checker inspired on Symmetric Delete algorithm. It retrieves tokens and utilizes
#'  distance metrics to compute possible derived words.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#symmetric-spellchecker}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param dictionary_path path to dictionary of properly written words
#' @param dictionary_token_pattern token pattern used in dictionary of properly written words
#' @param dictionary_read_as LINE_BY_LINE or SPARK_DATASET
#' @param dictionary_options options to pass to the Spark reader
#' @param max_edit_distance Maximum edit distance to calculate possible derived words. Defaults to 3.
#' @param dups_limit maximum duplicate of characters in a word to consider.
#' @param deletes_threshold minimum frequency of corrections a word needs to have to be considered from training.
#' @param frequency_threshold minimum frequency of words to be considered from training.
#' @param longest_word_length ength of longest word in corpus
#' @param max_frequency maximum frequency of a word in the corpus
#' @param min_frequency minimum frequency of a word in the corpus
#' 
#' @export
nlp_symmetric_delete <- function(x, input_cols, output_col,
                 dictionary_path = NULL, dictionary_token_pattern = "\\S+", dictionary_read_as = "LINE_BY_LINE", 
                 dictionary_options = list("format" = "text"), max_edit_distance = NULL, dups_limit = NULL, 
                 deletes_threshold = NULL, frequency_threshold = NULL, longest_word_length = NULL, 
                 max_frequency = NULL, min_frequency = NULL,
                 uid = random_string("symmetric_delete_")) {
  UseMethod("nlp_symmetric_delete")
}

#' @export
nlp_symmetric_delete.spark_connection <- function(x, input_cols, output_col,
                                                  dictionary_path = NULL, dictionary_token_pattern = "\\S+", dictionary_read_as = "LINE_BY_LINE", 
                                                  dictionary_options = list("format" = "text"), max_edit_distance = NULL, dups_limit = NULL, 
                                                  deletes_threshold = NULL, frequency_threshold = NULL, longest_word_length = NULL, 
                                                  max_frequency = NULL, min_frequency = NULL,
                                                  uid = random_string("symmetric_delete_")){
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    dictionary_path = dictionary_path,
    dictionary_token_pattern = dictionary_token_pattern,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    max_edit_distance = max_edit_distance,
    dups_limit = dups_limit,
    deletes_threshold = deletes_threshold,
    frequency_threshold = frequency_threshold,
    longest_word_length = longest_word_length,
    max_frequency = max_frequency,
    min_frequency = min_frequency,
    uid = uid
  ) %>%
  validator_nlp_symmetric_delete()
  
  if (!is.null(args[["dictionary_options"]])) {
    args[["dictionary_options"]] = list2env(args[["dictionary_options"]])
  }

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.spell.symmetric.SymmetricDeleteApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setMaxEditDistance", args[["max_edit_distance"]])  %>%
    sparklyr::jobj_set_param("setDupsLimit", args[["dups_limit"]])  %>%
    sparklyr::jobj_set_param("setDeletesThreshold", args[["deletes_threshold"]])  %>%
    sparklyr::jobj_set_param("setFrequencyThreshold", args[["frequency_threshold"]])  %>%
    sparklyr::jobj_set_param("setLongestWordLength", args[["longest_word_length"]])  %>%
    sparklyr::jobj_set_param("setMaxFrequency", args[["max_frequency"]])  %>%
    sparklyr::jobj_set_param("setMinFrequency", args[["min_frequency"]]) 
  
  if (!is.null(args[["dictionary_path"]])) {
    sparklyr::invoke(jobj, "setDictionary", args[["dictionary_path"]], args[["dictionary_token_pattern"]],
                     read_as(x, args[["dictionary_read_as"]]), args[["dictionary_options"]])
  }

  new_nlp_symmetric_delete(jobj)
}

#' @export
nlp_symmetric_delete.ml_pipeline <- function(x, input_cols, output_col,
                                             dictionary_path = NULL, dictionary_token_pattern = "\\S+", dictionary_read_as = "LINE_BY_LINE", 
                                             dictionary_options = list("format" = "text"), max_edit_distance = NULL, dups_limit = NULL, 
                                             deletes_threshold = NULL, frequency_threshold = NULL, longest_word_length = NULL, 
                                             max_frequency = NULL, min_frequency = NULL,
                                             uid = random_string("symmetric_delete_")) {

  stage <- nlp_symmetric_delete.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    dictionary_path = dictionary_path,
    dictionary_token_pattern = dictionary_token_pattern,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    max_edit_distance = max_edit_distance,
    dups_limit = dups_limit,
    deletes_threshold = deletes_threshold,
    frequency_threshold = frequency_threshold,
    longest_word_length = longest_word_length,
    max_frequency = max_frequency,
    min_frequency = min_frequency,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_symmetric_delete.tbl_spark <- function(x, input_cols, output_col,
                                           dictionary_path = NULL, dictionary_token_pattern = "\\S+", dictionary_read_as = "LINE_BY_LINE", 
                                           dictionary_options = list("format" = "text"), max_edit_distance = NULL, dups_limit = NULL, 
                                           deletes_threshold = NULL, frequency_threshold = NULL, longest_word_length = NULL, 
                                           max_frequency = NULL, min_frequency = NULL,
                                           uid = random_string("symmetric_delete_")) {
  stage <- nlp_symmetric_delete.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    dictionary_path = dictionary_path,
    dictionary_token_pattern = dictionary_token_pattern,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    max_edit_distance = max_edit_distance,
    dups_limit = dups_limit,
    deletes_threshold = deletes_threshold,
    frequency_threshold = frequency_threshold,
    longest_word_length = longest_word_length,
    max_frequency = max_frequency,
    min_frequency = min_frequency,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}

#' Load a pretrained Spark NLP model
#' 
#' Create a pretrained Spark NLP \code{SymmetricDeleteModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @export
nlp_symmetric_delete_pretrained <- function(sc, input_cols, output_col,
                                                name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  ) %>%
    validator_nlp_symmetric_delete()
  
  model_class <- "com.johnsnowlabs.nlp.annotators.spell.symmetric.SymmetricDeleteModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])
  
  new_nlp_symmetric_delete_model(model)
}

#' @import forge
validator_nlp_symmetric_delete <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["dictionary_path"]] <- cast_nullable_string(args[["dictionary_path"]])
  args[["dictionary_token_pattern"]] <- cast_nullable_string(args[["dictionary_token_pattern"]])
  args[["max_edit_distance"]] <- cast_nullable_integer(args[["max_edit_distance"]])
  args[["dups_limit"]] <- cast_nullable_integer(args[["dups_limit"]])
  args[["deletes_threshold"]] <- cast_nullable_integer(args[["deletes_threshold"]])
  args[["frequency_threshold"]] <- cast_nullable_integer(args[["frequency_threshold"]])
  args[["longest_word_length"]] <- cast_nullable_integer(args[["longest_word_length"]])
  args[["max_frequency"]] <- cast_nullable_integer(args[["max_frequency"]])
  args[["min_frequency"]] <- cast_nullable_integer(args[["min_frequency"]])
  args
}

new_nlp_symmetric_delete <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_symmetric_delete")
}

new_nlp_symmetric_delete_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_symmetric_delete_model")
}

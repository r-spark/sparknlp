#' Spark NLP StopWordsCleaner
#'
#' Spark ML transformer that excludes from a sequence of strings (e.g. the output of a Tokenizer, Normalizer, 
#' Lemmatizer, and Stemmer) and drops all the stop words from the input sequences.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#stopwordscleaner}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param case_sensitive Whether to do a case sensitive comparison over the stop words.
#' @param locale Locale of the input for case insensitive matching. Ignored when caseSensitive is true.
#' @param stop_words The words to be filtered out.
#' 
#' @export
nlp_stop_words_cleaner <- function(x, input_cols, output_col,
                 case_sensitive = NULL, locale = NULL, stop_words = NULL,
                 uid = random_string("stop_words_cleaner_")) {
  UseMethod("nlp_stop_words_cleaner")
}

#' @export
nlp_stop_words_cleaner.spark_connection <- function(x, input_cols, output_col,
                 case_sensitive = NULL, locale = NULL, stop_words = NULL,
                 uid = random_string("stop_words_cleaner_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    case_sensitive = case_sensitive,
    locale = locale,
    stop_words = stop_words,
    uid = uid
  ) %>%
  validator_nlp_stop_words_cleaner()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.StopWordsCleaner",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setCaseSensitive", args[["case_sensitive"]])  %>%
    sparklyr::jobj_set_param("setLocale", args[["locale"]])  %>%
    sparklyr::jobj_set_param("setStopWords", args[["stop_words"]]) 

  new_nlp_stop_words_cleaner(jobj)
}

#' @export
nlp_stop_words_cleaner.ml_pipeline <- function(x, input_cols, output_col,
                 case_sensitive = NULL, locale = NULL, stop_words = NULL,
                 uid = random_string("stop_words_cleaner_")) {

  stage <- nlp_stop_words_cleaner.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    case_sensitive = case_sensitive,
    locale = locale,
    stop_words = stop_words,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_stop_words_cleaner.tbl_spark <- function(x, input_cols, output_col,
                 case_sensitive = NULL, locale = NULL, stop_words = NULL,
                 uid = random_string("stop_words_cleaner_")) {
  stage <- nlp_stop_words_cleaner.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    case_sensitive = case_sensitive,
    locale = locale,
    stop_words = stop_words,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_stop_words_cleaner <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["case_sensitive"]] <- cast_nullable_logical(args[["case_sensitive"]])
  args[["locale"]] <- cast_nullable_string(args[["locale"]])
  args[["stop_words"]] <- cast_nullable_string_list(args[["stop_words"]])
  args
}

new_nlp_stop_words_cleaner <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_stop_words_cleaner")
}

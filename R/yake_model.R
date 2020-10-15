#' Spark NLP YakeModel
#'
#' Spark ML estimator that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#yakemodel-keywords-extraction}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param min_ngrams select the minimum length of an extracted keyword
#' @param max_ngrams select the maximum length of an extracted keyword
#' @param n_keywords extract the top N keywords
#' @param stop_words set the list of stop words
#' @param threshold each keyword will be given a keyword score greater than 0. (Lower the score better the keyword) Set an upper bound for the keyword score from this method.
#' @param window_size Yake will construct a co-occurence matrix. You can set the window size for the cooccurence matrix construction from this method. ex: windowSize=2 will look at two words to both left and right of a candidate word. 
#' 
#' @export
nlp_yake_model <- function(x, input_cols, output_col,
                 min_ngrams = NULL, max_ngrams = NULL, n_keywords = NULL, 
                 stop_words = NULL, threshold = NULL, window_size = NULL,
                 uid = random_string("yake_model_")) {
  UseMethod("nlp_yake_model")
}

#' @export
nlp_yake_model.spark_connection <- function(x, input_cols, output_col,
                 min_ngrams = NULL, max_ngrams = NULL, n_keywords = NULL, 
                 stop_words = NULL, threshold = NULL, window_size = NULL,
                 uid = random_string("yake_model_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    min_ngrams = min_ngrams,
    max_ngrams = max_ngrams,
    n_keywords = n_keywords,
    stop_words = stop_words,
    threshold = threshold,
    window_size = window_size,
    uid = uid
  ) %>%
  validator_nlp_yake_model()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.keyword.yake.YakeModel",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setMinNGrams", args[["min_ngrams"]])  %>%
    sparklyr::jobj_set_param("setMaxNGrams", args[["max_ngrams"]])  %>%
    sparklyr::jobj_set_param("setNKeywords", args[["n_keywords"]])  %>%
    sparklyr::jobj_set_param("setStopWords", args[["stop_words"]])  %>%
    sparklyr::jobj_set_param("setWindowSize", args[["window_size"]]) 

  annotator <- new_nlp_yake_model(jobj)
  
  if (!is.null(args[["threshold"]])) {
    nlp_set_param(annotator, "threshold", args[["threshold"]])
  }
  
  return(annotator)
}

#' @export
nlp_yake_model.ml_pipeline <- function(x, input_cols, output_col,
                 min_ngrams = NULL, max_ngrams = NULL, n_keywords = NULL, stop_words = NULL, threshold = NULL, window_size = NULL,
                 uid = random_string("yake_model_")) {

  stage <- nlp_yake_model.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    min_ngrams = min_ngrams,
    max_ngrams = max_ngrams,
    n_keywords = n_keywords,
    stop_words = stop_words,
    threshold = threshold,
    window_size = window_size,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_yake_model.tbl_spark <- function(x, input_cols, output_col,
                 min_ngrams = NULL, max_ngrams = NULL, n_keywords = NULL, stop_words = NULL, threshold = NULL, window_size = NULL,
                 uid = random_string("yake_model_")) {
  stage <- nlp_yake_model.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    min_ngrams = min_ngrams,
    max_ngrams = max_ngrams,
    n_keywords = n_keywords,
    stop_words = stop_words,
    threshold = threshold,
    window_size = window_size,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_yake_model <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["min_ngrams"]] <- cast_nullable_integer(args[["min_ngrams"]])
  args[["max_ngrams"]] <- cast_nullable_integer(args[["max_ngrams"]])
  args[["n_keywords"]] <- cast_nullable_integer(args[["n_keywords"]])
  args[["stop_words"]] <- cast_nullable_string_list(args[["stop_words"]])
  args[["threshold"]] <- cast_nullable_double(args[["threshold"]])
  args[["window_size"]] <- cast_nullable_integer(args[["window_size"]])
  args
}

nlp_float_params.nlp_yake_model <- function(x) {
  return(c("threshold"))
}

new_nlp_yake_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_yake_model")
}

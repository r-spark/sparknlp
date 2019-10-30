#' Spark NLP SentimentDetector
#'
#' Spark ML estimator that scores a sentence for a sentiment
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#sentimentdetector}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param decrement_multiplier defaults to -2.0
#' @param dictionary_path path to file with list of inputs and their content
#' @param dictionary_delimiter delimiter in dictionary file
#' @param dictionary_read_as LINE_BY_LINE or SPARK_DATASET
#' @param dictionary_options options to pass to the Spark reader. Defaults to {"format" = "text"}
#' @param enable_score 
#' @param increment_multiplier defaults to 2.0
#' @param negative_multiplier defaults to -1.0
#' @param positive_multiplier defaults to 1.0
#' @param reverse_multiplier defaults to -1.0
#' 
#' @export
nlp_sentiment_detector <- function(x, input_cols, output_col,
                 decrement_multiplier = NULL, dictionary_path, dictionary_delimiter = ",", 
                 dictionary_read_as = "LINE_BY_LINE", dictionary_options = list("format" = "text"), 
                 enable_score = NULL, increment_multiplier = NULL, negative_multiplier = NULL, positive_multiplier = NULL, reverse_multiplier = NULL,
                 uid = random_string("sentiment_detector_")) {
  UseMethod("nlp_sentiment_detector")
}

#' @export
nlp_sentiment_detector.spark_connection <- function(x, input_cols, output_col,
                                                    decrement_multiplier = NULL, dictionary_path, dictionary_delimiter = ",", 
                                                    dictionary_read_as = "LINE_BY_LINE", dictionary_options = list("format" = "text"), 
                                                    enable_score = NULL, increment_multiplier = NULL, negative_multiplier = NULL, positive_multiplier = NULL, reverse_multiplier = NULL,
                                                    uid = random_string("sentiment_detector_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    decrement_multiplier = decrement_multiplier,
    dictionary_path = dictionary_path,
    dictionary_delimiter = dictionary_delimiter,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    enable_score = enable_score,
    increment_multiplier = increment_multiplier,
    negative_multiplier = negative_multiplier,
    positive_multiplier = positive_multiplier,
    reverse_multiplier = reverse_multiplier,
    uid = uid
  ) %>%
  validator_nlp_sentiment_detector()
  
  if (!is.null(args[["dictionary_options"]])) {
    args[["dictionary_options"]] <- list2env(args[["dictionary_options"]])
  }

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.sda.pragmatic.SentimentDetector",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setDecrementMultiplier", args[["decrement_multiplier"]])  %>%
    sparklyr::jobj_set_param("setEnableScore", args[["enable_score"]])  %>%
    sparklyr::jobj_set_param("setIncrementMultiplier", args[["increment_multiplier"]])  %>%
    sparklyr::jobj_set_param("setNegativeMultiplier", args[["negative_multiplier"]])  %>%
    sparklyr::jobj_set_param("setPositiveMultiplier", args[["positive_multiplier"]])  %>%
    sparklyr::jobj_set_param("setReverseMultiplier", args[["reverse_multiplier"]]) 

  if (!is.null(args[["dictionary_path"]])) {
    sparklyr::invoke(jobj, "setDictionary", args[["dictionary_path"]], args[["dictionary_delimiter"]],
                     read_as(x, args[["dictionary_read_as"]]), args[["dictionary_options"]])
  }
  
  new_nlp_sentiment_detector(jobj)
}

#' @export
nlp_sentiment_detector.ml_pipeline <- function(x, input_cols, output_col,
                                               decrement_multiplier = NULL, dictionary_path, dictionary_delimiter = ",", 
                                               dictionary_read_as = "LINE_BY_LINE", dictionary_options = list("format" = "text"), 
                                               enable_score = NULL, increment_multiplier = NULL, negative_multiplier = NULL, positive_multiplier = NULL, reverse_multiplier = NULL,
                                               uid = random_string("sentiment_detector_")) {

  stage <- nlp_sentiment_detector.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    decrement_multiplier = decrement_multiplier,
    dictionary_path = dictionary_path,
    dictionary_delimiter = dictionary_delimiter,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    enable_score = enable_score,
    increment_multiplier = increment_multiplier,
    negative_multiplier = negative_multiplier,
    positive_multiplier = positive_multiplier,
    reverse_multiplier = reverse_multiplier,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_sentiment_detector.tbl_spark <- function(x, input_cols, output_col,
                                             decrement_multiplier = NULL, dictionary_path, dictionary_delimiter = ",", 
                                             dictionary_read_as = "LINE_BY_LINE", dictionary_options = list("format" = "text"), 
                                             enable_score = NULL, increment_multiplier = NULL, negative_multiplier = NULL, positive_multiplier = NULL, reverse_multiplier = NULL,
                                             uid = random_string("sentiment_detector_")) {
  stage <- nlp_sentiment_detector.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    decrement_multiplier = decrement_multiplier,
    dictionary_path = dictionary_path,
    dictionary_delimiter = dictionary_delimiter,
    dictionary_read_as = dictionary_read_as,
    dictionary_options = dictionary_options,
    enable_score = enable_score,
    increment_multiplier = increment_multiplier,
    negative_multiplier = negative_multiplier,
    positive_multiplier = positive_multiplier,
    reverse_multiplier = reverse_multiplier,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}
#' @import forge
validator_nlp_sentiment_detector <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["decrement_multiplier"]] <- cast_nullable_double(args[["decrement_multiplier"]])
  args[["dictionary_path"]] <- cast_string(args[["dictionary_path"]])
  args[["dictionary_delimiter"]] <- cast_string(args[["dictionary_delimiter"]])
  args[["dictionary_read_as"]] <- cast_choice(args[["dictionary_read_as"]], choices = c("LINE_BY_LINE", "SPARK_DATASET"))
  args[["enable_score"]] <- cast_nullable_logical(args[["enable_score"]])
  args[["increment_multiplier"]] <- cast_nullable_double(args[["increment_multiplier"]])
  args[["negative_multiplier"]] <- cast_nullable_double(args[["negative_multiplier"]])
  args[["positive_multiplier"]] <- cast_nullable_double(args[["positive_multiplier"]])
  args[["reverse_multiplier"]] <- cast_nullable_double(args[["reverse_multiplier"]])
  args
}

new_nlp_sentiment_detector <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_sentiment_detector")
}

#' Spark NLP ViveknSentimentApproach
#'
#' Spark ML estimator that scores a sentence for a sentiment
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#viveknsentimentdetector}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param sentiment_col Column with sentiment analysis row’s result for training.
#' @param prune_corpus when training on small data you may want to disable this to not cut off infrequent words
#' @param feature_limit 
#' @param unimportant_feature_step 
#' @param important_feature_ratio 
#' 
#' @export
nlp_vivekn_sentiment_detector <- function(x, input_cols, output_col,
                 sentiment_col, prune_corpus = NULL, feature_limit = NULL, unimportant_feature_step = NULL, important_feature_ratio = NULL,
                 uid = random_string("vivekn_sentiment_detector_")) {
  UseMethod("nlp_vivekn_sentiment_detector")
}

#' @export
nlp_vivekn_sentiment_detector.spark_connection <- function(x, input_cols, output_col,
                 sentiment_col, prune_corpus = NULL, feature_limit = NULL, unimportant_feature_step = NULL, important_feature_ratio = NULL,
                 uid = random_string("vivekn_sentiment_detector_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    sentiment_col = sentiment_col,
    prune_corpus = prune_corpus,
    feature_limit = feature_limit,
    unimportant_feature_step = unimportant_feature_step,
    important_feature_ratio = important_feature_ratio,
    uid = uid
  ) %>%
  validator_nlp_vivekn_sentiment_detector()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setSentimentCol", args[["sentiment_col"]])  %>%
    sparklyr::jobj_set_param("setCorpusPrune", args[["prune_corpus"]])  %>%
    sparklyr::jobj_set_param("setFeatureLimit", args[["feature_limit"]])  %>%
    sparklyr::jobj_set_param("setUnimportantFeatureStep", args[["unimportant_feature_step"]])  %>%
    sparklyr::jobj_set_param("setImportantFeatureRatio", args[["important_feature_ratio"]]) 

  new_nlp_vivekn_sentiment_detector(jobj)
}

#' @export
nlp_vivekn_sentiment_detector.ml_pipeline <- function(x, input_cols, output_col,
                 sentiment_col, prune_corpus = NULL, feature_limit = NULL, unimportant_feature_step = NULL, important_feature_ratio = NULL,
                 uid = random_string("vivekn_sentiment_detector_")) {

  stage <- nlp_vivekn_sentiment_detector.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    sentiment_col = sentiment_col,
    prune_corpus = prune_corpus,
    feature_limit = feature_limit,
    unimportant_feature_step = unimportant_feature_step,
    important_feature_ratio = important_feature_ratio,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_vivekn_sentiment_detector.tbl_spark <- function(x, input_cols, output_col,
                 sentiment_col, prune_corpus = NULL, feature_limit = NULL, unimportant_feature_step = NULL, important_feature_ratio = NULL,
                 uid = random_string("vivekn_sentiment_detector_")) {
  stage <- nlp_vivekn_sentiment_detector.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    sentiment_col = sentiment_col,
    prune_corpus = prune_corpus,
    feature_limit = feature_limit,
    unimportant_feature_step = unimportant_feature_step,
    important_feature_ratio = important_feature_ratio,
    uid = uid
  )

  stage %>% sparklyr::ml_fit(x)
}

#' Load a pretrained Spark NLP model
#' 
#' Create a pretrained Spark NLP \code{ViveknSentimentModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @export
nlp_vivekn_sentiment_pretrained <- function(sc, input_cols, output_col,
                                      name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  ) %>%
    validator_nlp_vivekn_sentiment_detector()

  model_class <- "com.johnsnowlabs.nlp.annotators.sda.vivekn.ViveknSentimentModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])
  
  new_nlp_vivekn_sentiment_detector_model(model)
}

#' @import forge
validator_nlp_vivekn_sentiment_detector <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["sentiment_col"]] <- cast_nullable_string(args[["sentiment_col"]])
  args[["prune_corpus"]] <- cast_nullable_integer(args[["prune_corpus"]])
  args[["feature_limit"]] <- cast_nullable_integer(args[["feature_limit"]])
  args[["unimportant_feature_step"]] <- cast_nullable_double(args[["unimportant_feature_step"]])
  args[["important_feature_ratio"]] <- cast_nullable_double(args[["important_feature_ratio"]])
  args
}

new_nlp_vivekn_sentiment_detector <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_vivekn_sentiment_detector")
}

new_nlp_vivekn_sentiment_detector_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_vivekn_sentiment_detector_model")
}


#' Spark NLP SentenceDetector - sentence boundary detector
#' 
#' Spark ML Transformer that finds sentence bounds in raw text. Applies rule from Pragmatic Segmenter
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#sentencedetector}
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param custom_bounds Custom sentence separator text. Optional.
#' @param use_custom_only Use only custom bounds without considering those of Pragmatic Segmenter. Defaults to false. Needs customBounds.
#' @param use_abbreviations Whether to consider abbreviation strategies for better accuracy but slower performance. Defaults to true.
#' @param explode_sentences Whether to split sentences into different Dataset rows. Useful for higher parallelism in fat rows. Defaults to false.
#' @param detect_lists whether to take lists into consideration at sentence detection
#' @param max_length set the maximum allowed length for each sentence
#' @param min_length set the minimum allowed length for each sentence
#' @param split_length length at which sentences will be forcibly set
#' 
#' @export
nlp_sentence_detector <- function(x, input_cols, output_col,
                 custom_bounds = NULL, use_custom_only = NULL, use_abbreviations = NULL,
                 explode_sentences = NULL, detect_lists = NULL, min_length = NULL,
                 max_length = NULL, split_length = NULL,
                                   uid = random_string("sentence_detector_")) {
  UseMethod("nlp_sentence_detector")
}

#' @export
nlp_sentence_detector.spark_connection <- function(x, input_cols, output_col,
                               custom_bounds = NULL, use_custom_only = NULL, use_abbreviations = NULL,
                               explode_sentences = NULL, detect_lists = NULL, min_length = NULL,
                               max_length = NULL, split_length = NULL,
                               uid = random_string("sentence_detector_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    custom_bounds = custom_bounds,
    use_custom_only = use_custom_only,
    use_abbreviations = use_abbreviations,
    explode_sentences = explode_sentences,
    detect_lists = detect_lists,
    min_length = min_length,
    max_length = max_length,
    split_length = split_length,
    uid = uid
  ) %>% 
   validator_nlp_sentence_detector()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetector",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setCustomBounds", args[["custom_bounds"]]) %>%
    sparklyr::jobj_set_param("setUseCustomBoundsOnly", args[["use_custom_only"]]) %>%
    sparklyr::jobj_set_param("setUseAbbreviations", args[["use_abbreviations"]]) %>%
    sparklyr::jobj_set_param("setExplodeSentences", args[["explode_sentences"]]) %>% 
    sparklyr::jobj_set_param("setDetectLists", args[["detect_lists"]]) %>% 
    sparklyr::jobj_set_param("setMinLength", args[["min_length"]]) %>% 
    sparklyr::jobj_set_param("setMaxLength", args[["max_length"]]) %>% 
    sparklyr::jobj_set_param("setSplitLength", args[["split_length"]])
  
  new_nlp_sentence_detector(jobj)
}

#' @export
nlp_sentence_detector.ml_pipeline <- function(x, input_cols, output_col,
                                              custom_bounds = NULL, use_custom_only = NULL, use_abbreviations = NULL,
                                              explode_sentences = NULL, detect_lists = NULL, min_length = NULL,
                                              max_length = NULL, split_length = NULL,
                                              uid = random_string("sentence_detector_")) {

  stage <- nlp_sentence_detector.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    custom_bounds = custom_bounds,
    use_custom_only = use_custom_only,
    use_abbreviations = use_abbreviations,
    explode_sentences = explode_sentences,
    detect_lists = detect_lists,
    min_length = min_length,
    max_length = max_length,
    split_length = split_length,
    uid = uid
  )
  
  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_sentence_detector.tbl_spark <- function(x, input_cols, output_col,
                                            custom_bounds = NULL, use_custom_only = NULL, use_abbreviations = NULL,
                                            explode_sentences = NULL, detect_lists = NULL, min_length = NULL,
                                            max_length = NULL, split_length = NULL,
                                             uid = random_string("sentence_detector_")) {
  stage <- nlp_sentence_detector.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    custom_bounds = custom_bounds,
    use_custom_only = use_custom_only,
    use_abbreviations = use_abbreviations,
    explode_sentences = explode_sentences,
    detect_lists = detect_lists,
    min_length = min_length,
    max_length = max_length,
    split_length = split_length,
    uid = uid
  )
  
  stage %>% 
    sparklyr::ml_transform(x)
}

#' @import forge
validator_nlp_sentence_detector <- function(args) {
  # Input checking, much of these can be factored out; can be composed
  #   with other input checkers to avoid redundancy
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["custom_bounds"]] <- cast_nullable_string_list(args[["custom_bounds"]])
  args[["use_custom_only"]] <- cast_nullable_logical(args[["use_custom_only"]])
  args[["use_abbreviations"]] <- cast_nullable_logical(args[["use_abbreviations"]])
  args[["explode_sentences"]] <- cast_nullable_logical(args[["explode_sentences"]])
  args[["detect_lists"]] <- cast_nullable_logical(args[["detect_lists"]])
  args[["min_length"]] <- cast_nullable_integer(args[["min_length"]])
  args[["max_length"]] <- cast_nullable_integer(args[["max_length"]])
  args[["split_length"]] <- cast_nullable_integer(args[["split_length"]])
  args
}

new_nlp_sentence_detector <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_sentence_detector")
}
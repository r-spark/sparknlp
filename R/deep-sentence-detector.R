#' Spark NLP DeepSentenceDetector
#'
#' Spark ML transformer that finds sentence bounds in raw text. Applies a Named Entity Recognition DL model.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#deepsentencedetector}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param include_pragmatic_segmenter Whether to include rule-based sentence detector as first filter. Defaults to false.
#' @param end_punctuation An array of symbols that deep sentence detector will consider as an end of sentence punctuation. Defaults to “.”, “!”, “?”
#' @param custom_bounds 
#' @param explode_sentences 
#' @param max_length 
#' @param use_abbreviations 
#' @param use_custom_only 
#' 
#' @export
nlp_deep_sentence_detector <- function(x, input_cols, output_col,
                 include_pragmatic_segmenter = NULL, end_punctuation = NULL, custom_bounds = NULL, 
                 explode_sentences = NULL, max_length = NULL, use_abbreviations = NULL, use_custom_only = NULL,
                 uid = random_string("deep_sentence_detector_")) {
  UseMethod("nlp_deep_sentence_detector")
}

#' @export
nlp_deep_sentence_detector.spark_connection <- function(x, input_cols, output_col,
                 include_pragmatic_segmenter = NULL, end_punctuation = NULL, custom_bounds = NULL, 
                 explode_sentences = NULL, max_length = NULL, use_abbreviations = NULL, use_custom_only = NULL,
                 uid = random_string("deep_sentence_detector_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    include_pragmatic_segmenter = include_pragmatic_segmenter,
    end_punctuation = end_punctuation,
    custom_bounds = custom_bounds,
    explode_sentences = explode_sentences,
    max_length = max_length,
    use_abbreviations = use_abbreviations,
    use_custom_only = use_custom_only,
    uid = uid
  ) %>%
  validator_nlp_deep_sentence_detector()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.sbd.deep.DeepSentenceDetector",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setIncludePragmaticSegmenter", args[["include_pragmatic_segmenter"]])  %>%
    sparklyr::jobj_set_param("setEndPunctuation", args[["end_punctuation"]])  %>%
    sparklyr::jobj_set_param("setCustomBounds", args[["custom_bounds"]])  %>%
    sparklyr::jobj_set_param("setExplodeSentences", args[["explode_sentences"]])  %>%
    sparklyr::jobj_set_param("setMaxLength", args[["max_length"]])  %>%
    sparklyr::jobj_set_param("setUseAbbreviations", args[["use_abbreviations"]])  %>%
    sparklyr::jobj_set_param("setUseCustomBoundsOnly", args[["use_custom_only"]]) 

  new_nlp_deep_sentence_detector(jobj)
}

#' @export
nlp_deep_sentence_detector.ml_pipeline <- function(x, input_cols, output_col,
                 include_pragmatic_segmenter = NULL, end_punctuation = NULL, custom_bounds = NULL, 
                 explode_sentences = NULL, max_length = NULL, use_abbreviations = NULL, use_custom_only = NULL,
                 uid = random_string("deep_sentence_detector_")) {

  stage <- nlp_deep_sentence_detector.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    include_pragmatic_segmenter = include_pragmatic_segmenter,
    end_punctuation = end_punctuation,
    custom_bounds = custom_bounds,
    explode_sentences = explode_sentences,
    max_length = max_length,
    use_abbreviations = use_abbreviations,
    use_custom_only = use_custom_only,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_deep_sentence_detector.tbl_spark <- function(x, input_cols, output_col,
                 include_pragmatic_segmenter = NULL, end_punctuation = NULL, custom_bounds = NULL, 
                 explode_sentences = NULL, max_length = NULL, use_abbreviations = NULL, use_custom_only = NULL,
                 uid = random_string("deep_sentence_detector_")) {
  stage <- nlp_deep_sentence_detector.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    include_pragmatic_segmenter = include_pragmatic_segmenter,
    end_punctuation = end_punctuation,
    custom_bounds = custom_bounds,
    explode_sentences = explode_sentences,
    max_length = max_length,
    use_abbreviations = use_abbreviations,
    use_custom_only = use_custom_only,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_deep_sentence_detector <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["include_pragmatic_segmenter"]] <- cast_nullable_logical(args[["include_pragmatic_segmenter"]])
  args[["end_punctuation"]] <- cast_nullable_string_list(args[["end_punctuation"]])
  args[["custom_bounds"]] <- cast_nullable_string_list(args[["custom_bounds"]])
  args[["explode_sentences"]] <- cast_nullable_logical(args[["explode_sentences"]])
  args[["max_length"]] <- cast_nullable_integer(args[["max_length"]])
  args[["use_abbreviations"]] <- cast_nullable_logical(args[["use_abbreviations"]])
  args[["use_custom_only"]] <- cast_nullable_logical(args[["use_custom_only"]])
  args
}

new_nlp_deep_sentence_detector <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_deep_sentence_detector")
}

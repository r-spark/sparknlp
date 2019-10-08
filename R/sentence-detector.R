#' Spark NLP SentenceDetector - sentence boundary detector
#' 
#' Spark ML Transformer that finds sentence bounds in raw text. Applies rule from Pragmatic Segmenter
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#sentencedetector}
#' 
#' @param x A Spark connection, pipeline object, or a Spark data frame.
#' @param input_cols Input columns. Required.
#' @param output_col Output column. Required.
#' @param custom_bounds Custom sentence separator text. Optional.
#' @param use_custom_only Use only custom bounds without considering those of Pragmatic Segmenter. Defaults to false. Needs customBounds.
#' @param use_abbreviations Whether to consider abbreviation strategies for better accuracy but slower performance. Defaults to true.
#' @param explode_sentences Whether to split sentences into different Dataset rows. Useful for higher parallelism in fat rows. Defaults to false.
#' @param uid UID
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a SentenceDetector Transformer. When
#' \code{x} is a \code{ml_pipeline} the pipeline with the SentenceDetector added. When \code{x} is a 
#' \code{tbl_spark} a transformed \code{tbl_spark} (note that the Dataframe passed in must contain the input_cols specified).
#' 
#' @export
nlp_sentence_detector <- function(x, input_cols, output_col,
                 custom_bounds = NULL, use_custom_only = NULL, use_abbreviations = NULL, explode_sentences = NULL,
                                   uid = random_string("sentence_detector_")) {
  UseMethod("nlp_sentence_detector")
}

#' @export
nlp_sentence_detector.spark_connection <- function(x, input_cols, output_col,
                               custom_bounds = NULL, use_custom_only = NULL, use_abbreviations = NULL, explode_sentences = NULL,
                                                   uid = random_string("sentence_detector_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    custom_bounds = custom_bounds,
    use_custom_only = use_custom_only,
    use_abbreviations = use_abbreviations,
    explode_sentences = explode_sentences,
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
    sparklyr::jobj_set_param("setExplodeSentences", args[["explode_sentences"]])

  new_nlp_sentence_detector(jobj)
}

#' @export
nlp_sentence_detector.ml_pipeline <- function(x, input_cols, output_col,
                                              custom_bounds = NULL, use_custom_only = NULL, use_abbreviations = NULL, explode_sentences = NULL,
                                              uid = random_string("sentence_detector_")) {

  stage <- nlp_sentence_detector.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    custom_bounds = custom_bounds,
    use_custom_only = use_custom_only,
    use_abbreviations = use_abbreviations,
    explode_sentences = explode_sentences,
    uid = uid
  )
  
  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_sentence_detector.tbl_spark <- function(x, input_cols, output_col,
                                            custom_bounds = NULL, use_custom_only = NULL, use_abbreviations = NULL, explode_sentences = NULL,
                                             uid = random_string("sentence_detector_")) {
  stage <- nlp_sentence_detector.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    custom_bounds = custom_bounds,
    use_custom_only = use_custom_only,
    use_abbreviations = use_abbreviations,
    explode_sentences = explode_sentences,
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
  args
}

new_nlp_sentence_detector <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_sentence_detector")
}
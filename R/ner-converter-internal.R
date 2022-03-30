#' Spark NLP NerConverterInternal
#'
#' Spark ML transformer that converts IOB or IOB2 representation of NER to user-friendly. 
#' 
#' @template roxlate-nlp-transformer
#' @template roxlate-inputs-output-params
#' @param preserve_position Whether to preserve the original position of the tokens 
#' in the original document or use the modified tokens
#' @param white_list If defined, list of entities to process. The rest will be ignored.
#'  Do not include IOB prefix on labels"
#' @param black_list If defined, list of entities to ignore
#' @param lazy_annotator allows annotators to stand idle in the Pipeline and do nothing.
#'  Can be called by other Annotators in a RecursivePipeline
#' @param greedy_mode whether to ignore B tags for contiguous tokens of same entity same
#' @param threshold set the confidence threshold
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a NerConverter transformer.
#' When \code{x} is a \code{ml_pipeline} the pipeline with the NerConverter added. When \code{x}
#' is a \code{tbl_spark} a transformed \code{tbl_spark}  (note that the Dataframe passed in must have the input_cols specified).
#' 
#' @export
nlp_ner_converter_internal <- function(x, input_cols, output_col,
                 white_list = NULL, black_list = NULL, preserve_position = NULL, lazy_annotator = NULL,
                 greedy_mode = NULL, threshold = NULL, 
                 uid = random_string("ner_converter_internal_")) {
  UseMethod("nlp_ner_converter_internal")
}

#' @export
nlp_ner_converter_internal.spark_connection <- function(x, input_cols, output_col,
                 white_list = NULL, black_list = NULL,  preserve_position = NULL, lazy_annotator = NULL,
                 greedy_mode = NULL, threshold = NULL, 
                 uid = random_string("ner_converter_internal_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    preserve_position = preserve_position,
    white_list = white_list,
    black_list = black_list,
    lazy_annotator = lazy_annotator,
    greedy_mode = greedy_mode,
    threshold = threshold,
    uid = uid
  ) %>%
  validator_nlp_ner_converter_internal()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.ner.NerConverterInternal",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setWhiteList", args[["white_list"]]) %>% 
    sparklyr::jobj_set_param("setBlackList", args[["black_list"]]) %>%
    sparklyr::jobj_set_param("setPreservePosition", args[["preserve_position"]]) %>% 
    sparklyr::jobj_set_param("setLazyAnnotator", args[["lazy_annotator"]]) %>% 
    sparklyr::jobj_set_param("setGreedyMode", args[["greedy_mode"]])
  
  annotator <- new_nlp_ner_converter_internal(jobj)
  
  if (!is.null(args[["threshold"]])) {
    annotator <- nlp_set_param(annotator, "threshold", args[["threshold"]])
  }
  
  return(annotator)

}

#' @export
nlp_ner_converter_internal.ml_pipeline <- function(x, input_cols, output_col,
                 white_list = NULL, black_list = NULL,  preserve_position = NULL, lazy_annotator = NULL,
                 greedy_mode = NULL, threshold = NULL, 
                 uid = random_string("ner_converter_internal_")) {

  stage <- nlp_ner_converter_internal.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    white_list = white_list,
    black_list = black_list,
    preserve_position = preserve_position,
    lazy_annotator = lazy_annotator,
    greedy_mode = greedy_mode,
    threshold = threshold,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)

}

#' @export
nlp_ner_converter_internal.tbl_spark <- function(x, input_cols, output_col,
                 white_list = NULL, black_list = NULL, preserve_position = NULL, lazy_annotator = NULL,
                 greedy_mode = NULL, threshold = NULL, 
                 uid = random_string("ner_converter_internal_")) {
  stage <- nlp_ner_converter_internal.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    white_list = white_list,
    black_list = black_list,
    preserve_position = preserve_position,
    lazy_annotator = lazy_annotator,
    greedy_mode = greedy_mode,
    threshold = threshold,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}

nlp_float_params.nlp_ner_converter_internal <- function(x) {
  return(c("threshold"))
}

#' @import forge
validator_nlp_ner_converter_internal <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["white_list"]] <- cast_nullable_string_list(args[["white_list"]])
  args[["black_list"]] <- cast_nullable_string_list(args[["black_list"]])
  args[["preserve_position"]] <- cast_nullable_logical(args[["preserve_position"]])
  args[["lazy_annotator"]] <- cast_nullable_logical(args[["lazy_annotator"]])
  args[["greedy_mode"]] <- cast_nullable_logical(args[["greedy_mode"]])
  args[["threshold"]] <- cast_nullable_double(args[["threshold"]])
  args
}

new_nlp_ner_converter_internal <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_ner_converter_internal")
}

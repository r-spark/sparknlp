#' Spark NLP Finisher
#'
#' Spark ML transformer that 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#finisher}
#' 
#' @param x A Spark connection, pipeline object, or a Spark data frame.
#' @param input_cols Input column. Required.
#' @param output_cols Optional. Names for output columns.
#' @param clean_annotations Boolean. Whether to remove intermediate annotations
#' @param value_split_symbol String. Optional. Split values within an annotation character
#' @param annotation_split_symbol String. Optional. Split values between annotations character
#' @param include_metadata Boolean. Optional. Whether to include metadata keys. Sometimes useful in some annotations
#' @param output_as_array Boolean. Optional. Whether to output as Array. Useful as input for other Spark transformers
#' @param uid Optional. UID
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a Finisher transformer.
#' When \code{x} is a \code{ml_pipeline} the pipeline with the Finisher added. When \code{x}
#' is a \code{tbl_spark} a transformed \code{tbl_spark}  (note that the Dataframe passed in must have the input_cols specified).
#' 
#' @export
nlp_finisher <- function(x, input_cols, output_cols,
                 clean_annotations = NULL, value_split_symbol = NULL, annotation_split_symbol = NULL, include_metadata = NULL, output_as_array = NULL,
                 uid = random_string("finisher_")) {
  UseMethod("nlp_finisher")
}

#' @export
nlp_finisher.spark_connection <- function(x, input_cols, output_cols = NULL,
                 clean_annotations = NULL, value_split_symbol = NULL, annotation_split_symbol = NULL, include_metadata = NULL, output_as_array = NULL,
                 uid = random_string("finisher_")) {
  args <- list(
    input_cols = input_cols,
    output_cols = output_cols,
    clean_annotations = clean_annotations,
    value_split_symbol = value_split_symbol,
    annotation_split_symbol = annotation_split_symbol,
    include_metadata = include_metadata,
    output_as_array = output_as_array,
    uid = uid
  ) %>%
  validator_nlp_finisher()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.Finisher",
    input_cols = args[["input_cols"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setOutputCols", args[["output_cols"]]) %>%
    sparklyr::jobj_set_param("setCleanAnnotations", args[["clean_annotations"]])  %>%
    sparklyr::jobj_set_param("setValueSplitSymbol", args[["value_split_symbol"]])  %>%
    sparklyr::jobj_set_param("setAnnotationSplitSymbol", args[["annotation_split_symbol"]])  %>%
    sparklyr::jobj_set_param("setIncludeMetadata", args[["include_metadata"]])  %>%
    sparklyr::jobj_set_param("setOutputAsArray", args[["output_as_array"]]) 

  new_nlp_finisher(jobj)
}

#' @export
nlp_finisher.ml_pipeline <- function(x, input_cols, output_cols = NULL,
                 clean_annotations = NULL, value_split_symbol = NULL, annotation_split_symbol = NULL, include_metadata = NULL, output_as_array = NULL,
                 uid = random_string("finisher_")) {

  stage <- nlp_finisher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_cols = output_cols,
    clean_annotations = clean_annotations,
    value_split_symbol = value_split_symbol,
    annotation_split_symbol = annotation_split_symbol,
    include_metadata = include_metadata,
    output_as_array = output_as_array,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_finisher.tbl_spark <- function(x, input_cols, output_cols = NULL,
                 clean_annotations = NULL, value_split_symbol = NULL, annotation_split_symbol = NULL, include_metadata = NULL, output_as_array = NULL,
                 uid = random_string("finisher_")) {
  stage <- nlp_finisher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_cols = output_cols,
    clean_annotations = clean_annotations,
    value_split_symbol = value_split_symbol,
    annotation_split_symbol = annotation_split_symbol,
    include_metadata = include_metadata,
    output_as_array = output_as_array,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_finisher <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_cols"]] <- cast_nullable_string_list(args[["output_cols"]])
  args[["clean_annotations"]] <- cast_nullable_logical(args[["clean_annotations"]])
  args[["value_split_symbol"]] <- cast_nullable_string(args[["value_split_symbol"]])
  args[["annotation_split_symbol"]] <- cast_nullable_string(args[["annotation_split_symbol"]])
  args[["include_metadata"]] <- cast_nullable_logical(args[["include_metadata"]])
  args[["output_as_array"]] <- cast_nullable_logical(args[["output_as_array"]])
  args
}

new_nlp_finisher <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_finisher")
}

#' Spark NLP GraphFinisher
#'
#' Helper class to convert the knowledge graph from GraphExtraction into a generic format, such as RDF.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#graphfinisher}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-input-output-params
#' @param clean_annotations Whether to remove annotation columns (Default: true)
#' @param include_metadata Annotation metadata format (Default: false)
#' @param output_as_array Finisher generates an Array with the results instead of string (Default: true)
#' 
#' @export
nlp_graph_finisher <- function(x, input_col, output_col,
                 clean_annotations = NULL, include_metadata = NULL, output_as_array = NULL,
                 uid = random_string("graph_finisher_")) {
  UseMethod("nlp_graph_finisher")
}

#' @export
nlp_graph_finisher.spark_connection <- function(x, input_col, output_col,
                 clean_annotations = NULL, include_metadata = NULL, output_as_array = NULL,
                 uid = random_string("graph_finisher_")) {
  args <- list(
    input_col = input_col,
    output_col = output_col,
    clean_annotations = clean_annotations,
    include_metadata = include_metadata,
    output_as_array = output_as_array,
    uid = uid
  ) %>%
  validator_nlp_graph_finisher()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.GraphFinisher",
    input_col = args[["input_col"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setCleanAnnotations", args[["clean_annotations"]])  %>%
    sparklyr::jobj_set_param("setIncludeMetadata", args[["include_metadata"]])  %>%
    sparklyr::jobj_set_param("setOutputAsArray", args[["output_as_array"]]) 

  new_nlp_graph_finisher(jobj)
}

#' @export
nlp_graph_finisher.ml_pipeline <- function(x, input_col, output_col,
                 clean_annotations = NULL, include_metadata = NULL, output_as_array = NULL,
                 uid = random_string("graph_finisher_")) {

  stage <- nlp_graph_finisher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    clean_annotations = clean_annotations,
    include_metadata = include_metadata,
    output_as_array = output_as_array,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_graph_finisher.tbl_spark <- function(x, input_col, output_col,
                 clean_annotations = NULL, include_metadata = NULL, output_as_array = NULL,
                 uid = random_string("graph_finisher_")) {
  stage <- nlp_graph_finisher.spark_connection(
    x = sparklyr::spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    clean_annotations = clean_annotations,
    include_metadata = include_metadata,
    output_as_array = output_as_array,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_graph_finisher <- function(args) {
  args[["input_col"]] <- cast_string(args[["input_col"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["clean_annotations"]] <- cast_nullable_logical(args[["clean_annotations"]])
  args[["include_metadata"]] <- cast_nullable_logical(args[["include_metadata"]])
  args[["output_as_array"]] <- cast_nullable_logical(args[["output_as_array"]])
  args
}

nlp_float_params.nlp_graph_finisher <- function(x) {
  return(c())
}
new_nlp_graph_finisher <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_graph_finisher")
}

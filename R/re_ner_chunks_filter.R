#' Spark NLP RENerChunksFilter
#'
#' Spark ML transformer that filters and outputs combinations of relations between 
#' extracted entities, for further processing. This annotator is especially useful 
#' to create inputs for the RelationExtractionDLModel.
#' 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_annotators#renerchunksfilter}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param max_syntactic_distance Maximal syntactic distance, as threshold (Default: 0)
#' @param relation_pairs List of dash-separated pairs of named entities 
#' ("ENTITY1-ENTITY2", e.g. "Biomarker-RelativeDay"), which will be processed
#' 
#' @export
nlp_re_ner_chunks_filter <- function(x, input_cols, output_col,
                 max_syntactic_distance = NULL, relation_pairs,
                 uid = random_string("re_ner_chunks_filter_")) {
  UseMethod("nlp_re_ner_chunks_filter")
}

#' @export
nlp_re_ner_chunks_filter.spark_connection <- function(x, input_cols, output_col,
                 max_syntactic_distance = NULL, relation_pairs,
                 uid = random_string("re_ner_chunks_filter_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    max_syntactic_distance = max_syntactic_distance,
    relation_pairs = relation_pairs,
    uid = uid
  ) %>%
  validator_nlp_re_ner_chunks_filter()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.re.RENerChunksFilter",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setMaxSyntacticDistance", args[["max_syntactic_distance"]])  %>%
    sparklyr::jobj_set_param("setRelationPairs", args[["relation_pairs"]])

  new_nlp_re_ner_chunks_filter(jobj)
}

#' @export
nlp_re_ner_chunks_filter.ml_pipeline <- function(x, input_cols, output_col,
                 max_syntactic_distance = NULL, relation_pairs,
                 uid = random_string("re_ner_chunks_filter_")) {

  stage <- nlp_re_ner_chunks_filter.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    max_syntactic_distance = max_syntactic_distance,
    relation_pairs = relation_pairs,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_re_ner_chunks_filter.tbl_spark <- function(x, input_cols, output_col,
                 max_syntactic_distance = NULL, relation_pairs,
                 uid = random_string("re_ner_chunks_filter_")) {
  stage <- nlp_re_ner_chunks_filter.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    max_syntactic_distance = max_syntactic_distance,
    relation_pairs = relation_pairs,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_re_ner_chunks_filter <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["max_syntactic_distance"]] <- cast_nullable_integer(args[["max_syntactic_distance"]])
  args[["relation_pairs"]] <- cast_string_list(args[["relation_pairs"]])
  args
}

nlp_float_params.nlp_re_ner_chunks_filter <- function(x) {
  return(c())
}
new_nlp_re_ner_chunks_filter <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_re_ner_chunks_filter")
}

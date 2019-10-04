
# Each estimator/transformer is a generic that has implementations for
#   1. `spark_connection`
#   2. `ml_pipeline`
#   3. tbl_spark or whatever is natural to be passed to it to be
#         evaluated eagerly
#
#  Docs could be improved...
#
#' Document Assembler
#' 
#' @param x A Spark connection, pipeline object, or a Spark data frame.
#' @param input_col Input column.
#' @param output_col Output column.
#' @param id_col ID column.
#' @param metadata_col Metadata column.
#' @param uid UID
#' 
#' @export
nlp_document_assembler <- function(x, input_col = NULL, output_col = NULL,
                                   id_col = NULL, metadata_col = NULL, 
                                   uid = random_string("document_assembler_")) {
  UseMethod("nlp_document_assembler")
}

#' @export
nlp_document_assembler.spark_connection <- function(x, input_col = NULL, output_col = NULL,
                                   id_col = NULL, metadata_col = NULL, 
                                   uid = random_string("document_assembler_")) {
  args <- list(
    input_col = input_col,
    output_col = output_col,
    id_col = id_col,
    metadata_col = metadata_col,
    uid = uid
  ) %>%
    validator_nlp_document_assembler()
  
  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.DocumentAssembler",
    input_col = args[["input_col"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    # `jobj_set_param()` helps with potentially null values
    #   or parameters that require a minimum Spark version
    sparklyr::jobj_set_param("setIdCol", args[["id_col"]]) %>%
    sparklyr::jobj_set_param("setMetadataCol", args[["metadata_col"]])
  
  new_nlp_document_assembler(jobj)
}

#' @export
nlp_document_assembler.ml_pipeline <- function(x, input_col = NULL, output_col = NULL,
                                                    id_col = NULL, metadata_col = NULL,
                                                    uid = random_string("document_assembler_")) {

  stage <- nlp_document_assembler.spark_connection(
    x = sparklyr::spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    id_col = id_col,
    metadata_col = metadata_col,
    uid = uid
  )
  
  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_document_assembler.tbl_spark <- function(x, input_col = NULL, output_col = NULL,
                                             id_col = NULL, metadata_col = NULL, 
                                             uid = random_string("document_assembler_")) {
  stage <- nlp_document_assembler.spark_connection(
    x = sparklyr::spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    id_col = id_col,
    metadata_col = metadata_col,
    uid = uid
  )
  
  stage %>% 
    sparklyr::ml_transform(x)
}

#' @import forge
validator_nlp_document_assembler <- function(args) {
  # Input checking, much of these can be factored out; can be composed
  #   with other input checkers to avoid redundancy
  args[["input_col"]] <- cast_nullable_string(args[["input_col"]])
  args[["output_col"]] <- cast_nullable_string(args[["output_col"]])
  args[["id_col"]] <- cast_nullable_string(args[["id_col"]])
  args[["metadata_col"]] <- cast_nullable_string(args[["metadata_col"]])
  args
}

new_nlp_document_assembler <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_document")
}
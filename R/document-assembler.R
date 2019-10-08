#' Spark NLP DocumentAssembler
#' 
#' Spark ML transformer that creates the first annotation of type Document. Can read a column containing either
#' a \code{String} or \code{Array[String]}. See 
#' \url{https://nlp.johnsnowlabs.com/docs/en/annotators#documentassembler-getting-data-in}
#' 
#' @param x A Spark connection, pipeline object, or a Spark data frame.
#' @param input_col Input column. Required.
#' @param output_col Output column. Required.
#' @param id_col String type column with id information. Optional.
#' @param metadata_col Map type column with metadata information. Optional.
#' @param cleanup_mode Cleaning up options. Optional. Default is "disabled". Possible values:
#' \tabular{ll}{
#' disabled \tab source kept as original \cr
#' inplace \tab removes new lines and tabs \cr
#' inplace_full \tab removes new lines and tabs but also those which were converted to strings \cr
#' shrink \tab removes new lines and tabs, plus merging multiple spaces and blank lines to a single space. \cr
#' shrink_full \tab removews new lines and tabs, including stringified values, plus shrinking spaces and blank lines. \cr
#'  }
#' 
#' @param uid UID
#' 
#' @return When \code{x} is a \code{spark_connection} the function returns a DocumentAssembler Transformer. When
#' \code{x} is a \code{ml_pipeline} the pipeline with the DocumentAssembler added. When \code{x} is a 
#' \code{tbl_spark} a transformed \code{tbl_spark} (note that the Dataframe passed in must have the input_col specified).
#' 
#' @export
nlp_document_assembler <- function(x, input_col, output_col,
                                   id_col = NULL, metadata_col = NULL, cleanup_mode = NULL,
                                   uid = random_string("document_assembler_")) {
  UseMethod("nlp_document_assembler")
}

#' @export
nlp_document_assembler.spark_connection <- function(x, input_col, output_col,
                                   id_col = NULL, metadata_col = NULL, cleanup_mode = NULL,
                                   uid = random_string("document_assembler_")) {
  args <- list(
    input_col = input_col,
    output_col = output_col,
    id_col = id_col,
    metadata_col = metadata_col,
    cleanup_mode = cleanup_mode,
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
    sparklyr::jobj_set_param("setMetadataCol", args[["metadata_col"]]) %>% 
    sparklyr::jobj_set_param("setCleanupMode", args[["cleanup_mode"]])
  
  
  new_nlp_document_assembler(jobj)
}

#' @export
nlp_document_assembler.ml_pipeline <- function(x, input_col, output_col,
                                                    id_col = NULL, metadata_col = NULL, cleanup_mode = NULL,
                                                    uid = random_string("document_assembler_")) {

  stage <- nlp_document_assembler.spark_connection(
    x = sparklyr::spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    id_col = id_col,
    metadata_col = metadata_col,
    cleanup_mode = cleanup_mode,
    uid = uid
  )
  
  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_document_assembler.tbl_spark <- function(x, input_col, output_col,
                                             id_col = NULL, metadata_col = NULL, cleanup_mode = NULL,
                                             uid = random_string("document_assembler_")) {
  stage <- nlp_document_assembler.spark_connection(
    x = sparklyr::spark_connection(x),
    input_col = input_col,
    output_col = output_col,
    id_col = id_col,
    metadata_col = metadata_col,
    cleanup_mode = cleanup_mode,
    uid = uid
  )
  
  stage %>% 
    sparklyr::ml_transform(x)
}

#' @import forge
validator_nlp_document_assembler <- function(args) {
  # Input checking, much of these can be factored out; can be composed
  #   with other input checkers to avoid redundancy
  args[["input_col"]] <- cast_string(args[["input_col"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["id_col"]] <- cast_nullable_string(args[["id_col"]])
  args[["metadata_col"]] <- cast_nullable_string(args[["metadata_col"]])
  args[["cleanup_mode"]] <- cast_nullable_string(args[["cleanup_mode"]])
  args
}

new_nlp_document_assembler <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_document")
}
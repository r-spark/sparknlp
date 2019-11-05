#' Spark NLP Doc2Chunk
#'
#' Spark ML transformer that Converts DOCUMENT type annotations into CHUNK type with the contents of a chunkCol. 
#' Chunk text must be contained within input DOCUMENT. May be either StringType or ArrayType[StringType] 
#' (using isArray Param) Useful for annotators that require a CHUNK type input.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/transformers#doc2chunk}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param is_array Whether the target chunkCol is ArrayType<StringType>
#' @param chunk_col String or StringArray column with the chunks that belong to the inputCol target
#' @param start_col Target INT column pointing to the token index (split by white space)
#' @param start_col_by_token_index Whether to use token index by whitespace or character index in startCol
#' @param fail_on_missing Whether to fail when a chunk is not found within inputCol
#' @param lowercase whether to increase matching by lowercasing everything before matching
#' 
#' @export
nlp_doc2chunk <- function(x, input_cols, output_col,
                 is_array = NULL, chunk_col = NULL, start_col = NULL, start_col_by_token_index = NULL, fail_on_missing = NULL, lowercase = NULL,
                 uid = random_string("doc2chunk_")) {
  UseMethod("nlp_doc2chunk")
}

#' @export
nlp_doc2chunk.spark_connection <- function(x, input_cols, output_col,
                 is_array = NULL, chunk_col = NULL, start_col = NULL, start_col_by_token_index = NULL, fail_on_missing = NULL, lowercase = NULL,
                 uid = random_string("doc2chunk_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    is_array = is_array,
    chunk_col = chunk_col,
    start_col = start_col,
    start_col_by_token_index = start_col_by_token_index,
    fail_on_missing = fail_on_missing,
    lowercase = lowercase,
    uid = uid
  ) %>%
  validator_nlp_doc2chunk()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.Doc2Chunk",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setIsArray", args[["is_array"]])  %>%
    sparklyr::jobj_set_param("setChunkCol", args[["chunk_col"]])  %>%
    sparklyr::jobj_set_param("setStartCol", args[["start_col"]])  %>%
    sparklyr::jobj_set_param("setStartColByTokenIndex", args[["start_col_by_token_index"]])  %>%
    sparklyr::jobj_set_param("setFailOnMissing", args[["fail_on_missing"]])  %>%
    sparklyr::jobj_set_param("setLowerCase", args[["lowercase"]]) 

  new_nlp_doc2chunk(jobj)
}

#' @export
nlp_doc2chunk.ml_pipeline <- function(x, input_cols, output_col,
                 is_array = NULL, chunk_col = NULL, start_col = NULL, start_col_by_token_index = NULL, fail_on_missing = NULL, lowercase = NULL,
                 uid = random_string("doc2chunk_")) {

  stage <- nlp_doc2chunk.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    is_array = is_array,
    chunk_col = chunk_col,
    start_col = start_col,
    start_col_by_token_index = start_col_by_token_index,
    fail_on_missing = fail_on_missing,
    lowercase = lowercase,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_doc2chunk.tbl_spark <- function(x, input_cols, output_col,
                 is_array = NULL, chunk_col = NULL, start_col = NULL, start_col_by_token_index = NULL, fail_on_missing = NULL, lowercase = NULL,
                 uid = random_string("doc2chunk_")) {
  stage <- nlp_doc2chunk.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    is_array = is_array,
    chunk_col = chunk_col,
    start_col = start_col,
    start_col_by_token_index = start_col_by_token_index,
    fail_on_missing = fail_on_missing,
    lowercase = lowercase,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_doc2chunk <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["is_array"]] <- cast_nullable_logical(args[["is_array"]])
  args[["chunk_col"]] <- cast_nullable_string(args[["chunk_col"]])
  args[["start_col"]] <- cast_nullable_string(args[["start_col"]])
  args[["start_col_by_token_index"]] <- cast_nullable_logical(args[["start_col_by_token_index"]])
  args[["fail_on_missing"]] <- cast_nullable_logical(args[["fail_on_missing"]])
  args[["lowercase"]] <- cast_nullable_logical(args[["lowercase"]])
  args
}

new_nlp_doc2chunk <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_doc2chunk")
}

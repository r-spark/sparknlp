#' Spark NLP AnnotationToolJsonReader
#' 
#' The annotation tool json reader is a reader that generate a assertion train set from the json from annotations labs exports.
#' 
#' @param json_path path to the json from annotation lab export
#' @param assertion_labels list of strings
#' @param excluded_labels list of strings
#' @param cleanup_mode string (Default: disabled)
#' @param split_chars list of strings
#' @param context_chars list of strings
#' @param scheme string (Default: "IOB")
#' @param min_chars_tol integer (Default: 2)
#' @param align_chars_tol integer (Default: 1)
#' @param merge_overlapping boolean (Default: true)
#' @param sddl_path string (Default: "")
#' 
#' @return assertion train set
#' 
#' @export
nlp_annotation_tool_json_reader <- function(sc, json_path, assertion_labels = list(), excluded_labels = list(),
                                            cleanup_mode = "disabled", split_chars = list(), context_chars = list(),
                                            scheme = "IOB", min_chars_tol = 2L, align_chars_tol = 1L,
                                            merge_overlapping = TRUE, sddl_path = "") {

  min_chars_tol <- forge::cast_integer(min_chars_tol)
  align_chars_tol <- forge::cast_integer(align_chars_tol)
  split_chars <- forge::cast_string_list(split_chars)
  context_chars <- forge::cast_string_list(context_chars)
  excluded_labels <- forge::cast_string_list(excluded_labels)
  assertion_labels <- forge::cast_string_list(assertion_labels)
    
  reader <- sparklyr::invoke_new(sc, "com.johnsnowlabs.nlp.training.AnnotationToolJsonReader", assertion_labels, excluded_labels,
                                 cleanup_mode, split_chars, context_chars, scheme, min_chars_tol, align_chars_tol, merge_overlapping, sddl_path)
  
  dataset <- invoke(reader, "readDataset", sparklyr::spark_session(sc), json_path)
  
  return(dataset)
}

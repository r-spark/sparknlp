#' Spark NLP AnnotationToolJsonReader
#' 
#' The annotation tool json reader is a reader that generate a assertion train set from the json from annotations labs exports.
#' 
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
nlp_annotation_tool_json_reader <- function(sc, assertion_labels = list(), excluded_labels = list(),
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
  
  #dataset <- invoke(reader, "readDataset", sparklyr::spark_session(sc), json_path)
  
  return(reader)
}

#' Create a data frame from an AnnotationToolJsonReader
#'  
#' @param json_path path to the json from annotation lab export
#' @param reader an instance of AnnotationToolJsonReader \code{\link{nlp_annotation_tool_json_reader}}
#' 
#' @return assertion train set data frame
#' 
#' @export
nlp_annotation_read_dataset <- function(reader, json_path) {
  dataset <- sparklyr::invoke(reader, "readDataset", sparklyr::spark_session(sparklyr::spark_connection(reader)), json_path)
  
  return(sparklyr::sdf_register(dataset))
}

#' Generate an assertion training set from an AnnotationToolJsonReader
#' 
#' @param reader an instance of AnnotationToolJsonReader \code{\link{nlp_annotation_tool_json_reader}}
#' @param df a Spark Dataframe
#' @param sentence_col the name of the sentence column
#' @param assertion_col the name of the assertion column
#' 
#' @return assertion training set data frame
#' 
#' @export
nlp_generate_assertion_train_set <- function(reader, df, sentence_col = "sentence", assertion_col = "assertion_label") {
  obj <- sparklyr::invoke(reader, "generateAssertionTrainSet", sparklyr::spark_dataframe(df), sentence_col, assertion_col)
  return(sparklyr::sdf_register(obj))
}

#' Generate a CoNLL format file from a data frame using an AnnotationToolJsonReader
#' 
#' @param reader an instance of AnnotationToolJsonReader \code{\link{nlp_annotation_tool_json_reader}}
#' @param df a Spark Dataframe
#' @param task_col the name of the task column
#' @param token_col the name of the token column
#' @param ner_label the name of the ner label column
#' 
#' @return NULL
#' 
#' @export
nlp_generate_colln <- function(reader, df, path, task_col = "task_id", token_col = "token",
                               ner_label = "ner_label") {
  obj <- sparklyr::invoke(reader, "generateColln", sparklyr::spark_dataframe(df), path, task_col, token_col, ner_label)
  return(NULL)
}

#' Generate a plain assertion training set from an AnnotationToolJsonReader
#' 
#' @param reader an instance of AnnotationToolJsonReader \code{\link{nlp_annotation_tool_json_reader}}
#' @param df a Spark Dataframe
#' @param task_col the name of the task column
#' @param token_col the name of the token column
#' @param ner_label the name of the ner label column
#' @param assertion_col the name of the assertion column
#' 
#' @return assertion training set data frame
#' 
#' @export
nlp_generate_plain_assertion_train_set <- function(reader, df, task_col = "task_id", token_col = "token",
                                                   ner_label = "ner_label", assertion_label = "assertion_label") {
  obj <- sparklyr::invoke(reader, "generatePlainAssertionTrainSet", sparklyr::spark_dataframe(df), task_col, token_col, ner_label, assertion_label)
  return(sparklyr::sdf_register(obj))
}



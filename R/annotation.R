#' Spark NLP Annotation object
#' 
#' A Spark NLP annotation object has the following fields:
#' * annotatorType: the type of annotation (String)
#' * begin: the index of the first character under this annotation (integer)
#' * end: the index after the last character under this annotation (integer)
#' * metadata: associated metadata for this annotation (Map(String, String))
#' * result: the main output of the annotation (String)
#' * embeddings: vector of embeddings (Array(Float))
#' 
#'  See \url{https://nlp.johnsnowlabs.com/docs/en/concepts#annotation}
#'  
#' @param x a spark_jobj or list
#'  
#' @return an nlp_annotation object
#' 
#' @exportClass nlp_annotation
#' @export
nlp_annotation <- function(x) {
  UseMethod("nlp_annotation", x)
}

#' @export
nlp_annotation.spark_jobj <- function(x) {
  jobj_info <- sparklyr:::jobj_info(x)
  jobj_class <- jobj_info$class
  
  annotatorType <- invoke(x, "annotatorType")
  begin <- invoke(x, "begin")
  end <- invoke(x, "end")
  metadata <- invoke(x, "metadata")
  result <- invoke(x, "result")
  embeddings <- invoke(x, "embeddings")
  
  new_nlp_annotation(annotatorType, begin, end, metadata, result, embeddings)
}

#' @export
nlp_annotation.list <- function(x) {
  annotatorType <- x[["annotatorType"]]
  begin <- x[["begin"]]
  end <- x[["end"]]
  metadata <- x[["metadata"]]
  result <- x[["result"]]
  embeddings <- x[["embeddings"]]
  
  new_nlp_annotation(annotatorType, begin, end, metadata, result, embeddings)
}

#' @export
print.nlp_annotation <- function(x, ...) {
  metadata_keys <- names(x$metadata)
  
  metadata_string <- ""
  comma = ""
  for (key in metadata_keys) {
    metadata_string <- paste0(metadata_string, comma, key, ": ", x$metadata[[key]])
    comma = ", "
  }
  
  result <- paste0("Annotation(", x$annotatorType, ", ", x$begin, ", ", x$end, ", ", x$result, ", {", metadata_string, "})") 
  print(result)
}

#' @export
as_tibble.nlp_annotation <- function(x) {
  tibble::tibble(annotatorType = x$annotatorType,
                 begin = x$begin,
                 end = x$end,
                 metadata = list(x$metadata),
                 result = x$result,
                 embeddings = ifelse(is.null(x$embeddings), NA, list(x$embeddings)))
}

#' @export
as.data.frame.nlp_annotation <- function(x) {
  tibble::tibble(annotatorType = x$annotatorType,
                 begin = x$begin,
                 end = x$end,
                 metadata = I(list(x$metadata)),
                 result = x$result,
                 embeddings = ifelse(is.null(x$embeddings), NA, I(list(x$embeddings))))
}

new_nlp_annotation <- function(annotatorType, begin, end, metadata, result, embeddings) {
    obj <- list(annotatorType = annotatorType, 
                begin = begin, 
                end = end, 
                metadata = metadata, 
                result = result,
                embeddings = embeddings)
    class(obj) <- "nlp_annotation"
    return(obj)
}

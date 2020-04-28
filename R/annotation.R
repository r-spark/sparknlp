#' Spark NLP Annotation object
#' 
#' @details
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

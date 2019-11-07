#' Spark NLP EmbeddingsHelper load
#' 
#' Uses EmbedddingsHelper to load word embeddings for annotators. 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/concepts#embeddingshelper}
#' 
#' @param sc Spark connection
#' @param path path to the embeddings file(s)
#' @param format TEXT, BINARY, SPARKNLP
#' @param reference the name to give the embeddings so they can be referred to later
#' @param dims dimensions of the vectors
#' @param case_sensitive case sensitivity of the embeddings
#' 
#' @return a handle to the ClusterWordEmbeddings object in Spark
#' 
#' @export
#' @import forge
nlp_load_embeddings <- function(sc, path, format, reference, dims, case_sensitive = FALSE) {
  path <- cast_string(path)
  format <- cast_string(format)
  dims <- cast_integer(dims)
  case_sensitive <- cast_logical(case_sensitive)
  
  invoke_static(sc, "com.johnsnowlabs.nlp.embeddings.EmbeddingsHelper", "load", path, spark_session(sc), format, 
                reference, dims, case_sensitive)
}

#' Spark NLP EmbeddingsHelper save
#' 
#' Uses EmbedddingsHelper to load word embeddings for annotators. 
#' See \url{https://nlp.johnsnowlabs.com/docs/en/concepts#embeddingshelper}
#' 
#' @param sc Spark connection
#' @param path path to the embeddings file(s)
#' @param embeddings the embeddings to save. Should be a ClusterWordEmbeddings object
#' 
#' @return a handle to the ClusterWordEmbeddings object in Spark
#' 
#' @export 
nlp_save_embeddings <- function(sc, path, embeddings) {
  invoke_static(sc, "com.johnsnowlabs.nlp.embeddings.EmbeddingsHelper", "save", path, embeddings, spark_session(sc))
}


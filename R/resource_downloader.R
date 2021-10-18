#' SparkNLP ResourceDownloader functions
#' 
#' ResourceDownloader provides functions to easily look for pretrained models & pipelines 
#' inside Spark NLP. You can filter models or pipelines via language, version,
#' or the name of the annotator
#' 
#' @param sc a spark_connect object
#' @param lang language to restrict the results to
#' @param version Spark NLP version to restrict results to
#' 
#' @return a markdown table containing the models or pipelines filtered by the provided arguments
#' 
#' @name nlp_resource_downloader
#' @aliases ResourceDownloader 
NULL

#' @rdname nlp_resource_downloader
#' @export
nlp_show_public_pipelines <- function(sc, lang = NULL, version = NULL) {
  result <- sparklyr::invoke_static(sc, "com.johnsnowlabs.nlp.pretrained.PythonResourceDownloader", "showPublicPipelines", lang, version)
  return(result)
}

#' @param annotator name of annotator to restrict results
#' @rdname nlp_resource_downloader
#' @export
nlp_show_public_models <- function(sc, annotator = NULL, lang = NULL, version = NULL) {
  result <- sparklyr::invoke_static(sc, "com.johnsnowlabs.nlp.pretrained.PythonResourceDownloader", "showPublicModels", annotator, lang, version)
  return(result)
}

#' @param name name of object to clear
#' @param language language to clear
#' @param remote_loc remote_loc of models to clear
#' @rdname nlp_resource_downloader
#' @export
nlp_clear_cache <- function(sc, name = NULL, language = NULL, remote_loc = NULL) {
  result <- sparklyr::invoke_static(sc, "com.johnsnowlabs.nlp.pretrained.PythonResourceDownloader", "clearCache", name, language, remote_loc)
  return(result)
}

#' @rdname nlp_resource_downloader
#' @export
nlp_show_available_annotators <- function(sc) {
  result <- sparklyr::invoke_static(sc, "com.johnsnowlabs.nlp.pretrained.PythonResourceDownloader", "showAvailableAnnotators")
  return(result)
}

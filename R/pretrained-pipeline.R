#' Spark NLP Pretrained pipeline
#' 
#' Creates a Spark NLP pretrained pipeline. See 
#' \url{https://nlp.johnsnowlabs.com/api/index.html#com.johnsnowlabs.nlp.pretrained.PretrainedPipeline} for the 
#' default values for the parameters if left null
#' 
#' @param x a Spark connection, Spark dataframe or string or character vector
#' @param download_name the name of the pretrained pipeline to download and create
#' @param lang the language of the pipeline
#' @param source the source for the pipeline file
#' @param parse_embeddings_vectors whether to parse the embeddings vectors or not
#' @param disk_location optional location on disk that the pipeline should be loaded from
#' 
#' @return The object returned depends on the class of \code{x}.
#'
#' \itemize{
#'   \item \code{spark_connection}: When \code{x} is a \code{spark_connection}, the function returns an instance of 
#'   a \code{ml_pipeline} created from the pretrained pipeline.
#'
#'   \item \code{tbl_spark}: When \code{x} is a \code{tbl_spark}, a the pretrained pipeline is created and immediately 
#'   run on the provied dataframe using \code{ml_fit_and_transform} returning the transformed data frame.
#' }
#' 
#' @export
nlp_pretrained_pipeline <- function(x, download_name, lang = "en", source = "public/models", parse_embeddings_vectors = FALSE, disk_location = NULL) {
  UseMethod("nlp_pretrained_pipeline")
}

# Returns a pipeline
#' @export
nlp_pretrained_pipeline.spark_connection <- function(x, download_name, lang = "en", source = "public/models", 
                                                     parse_embeddings_vectors = FALSE, disk_location = NULL) {
  jobj <- invoke_static(x, "sparknlp.Utils", "pretrainedPipeline", download_name, lang, source, parse_embeddings_vectors, disk_location)
  new_nlp_pretrained_pipeline(jobj)
}

# Runs the pipeline on the data frame
#' @export
nlp_pretrained_pipeline.tbl_spark <- function(x, download_name, lang = "en", source = "public/models", 
                                              parse_embeddings_vectors = FALSE, disk_location = NULL) {
  sc <- spark_connection(x)
  pipeline <- nlp_pretrained_pipeline.spark_connection(sc, download_name, lang, source, parse_embeddings_vectors, disk_location)
  sdf_register(invoke(sparklyr::spark_jobj(pipeline), "transform", spark_dataframe(x)))
}

new_nlp_pretrained_pipeline <- function(jobj) {
  structure(list(.jobj = jobj), class = c("nlp_pretrained_pipeline", "ml_transformer"))
}

#' Get the PipelineModel from a Spark NLP pretrained pipeline
#' 
#' Spark NLP pretrained pipelines are not Spark ML pipeline models. This function
#' will retrieve the ML pipeline model from the pretrained pipeline object.
#' 
#' @param pretrained_pipeline the Spark NLP PretrainedPipeline object
#' @return the Spark ML pipeline model from the input
#' 
#' @export
as_pipeline_model <- function(pipeline) {
  UseMethod("as_pipeline_model")
}

#' @export
as_pipeline_model.nlp_pretrained_pipeline <- function(pretrained_pipeline) {
  pm <- sparklyr:::new_ml_pipeline_model(invoke(pretrained_pipeline$.jobj, "model"))
  return(pm)
}

#' @export
spark_jobj.nlp_pretrained_pipeline <- function(x, ...) {
  x$.jobj
}
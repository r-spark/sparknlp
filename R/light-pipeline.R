#' Spark NLP Light pipeline
#' 
#' LightPipelines are Spark ML pipelines converted into a single machine but multithreaded task, becoming more than 
#' 10x times faster for smaller amounts of data (small is relative, but 50k sentences is roughly a good maximum).
#' To use them, simply plug in a trained (fitted) pipeline.
#' 
#' @param pipelineModel a trained (fitted) pipeline
#' 
#' @return a LightPipeline object
#'  
#' @export
nlp_light_pipeline <- function(pipeline_model) {
  sc <- spark_connection(pipeline_model)
  jobj <- invoke_new(sc, "com.johnsnowlabs.nlp.LightPipeline", spark_jobj(pipeline_model))
  new_nlp_light_pipeline(jobj)
}

new_nlp_light_pipeline <- function(jobj) {
  structure(list(.jobj = jobj), class = c("nlp_light_pipeline"))
}
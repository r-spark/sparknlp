#' Spark NLP Light pipeline
#' 
#' LightPipelines are Spark ML pipelines converted into a single machine but multithreaded task, becoming more than 
#' 10x times faster for smaller amounts of data (small is relative, but 50k sentences is roughly a good maximum).
#' To use them, simply plug in a trained (fitted) pipeline.
#' 
#' @param x a trained (fitted) pipeline
#' @param parse_embeddings 
#' 
#' @return a LightPipeline object
#'  
#' @export
#' 
nlp_light_pipeline <- function(x, parse_embeddings = FALSE) {
  UseMethod("nlp_light_pipeline", x)
}

#' @export
nlp_light_pipeline.nlp_pretrained_pipeline <- function(x, parse_embeddings = FALSE) {
  new_nlp_light_pipeline(invoke(spark_jobj(x), "lightModel"))
}

#' @export
nlp_light_pipeline.ml_pipeline_model <- function(x, parse_embeddings = FALSE) {
  sc <- spark_connection(x)
  jobj <- invoke_new(sc, "com.johnsnowlabs.nlp.LightPipeline", spark_jobj(x), parse_embeddings)
  new_nlp_light_pipeline(jobj)
}

new_nlp_light_pipeline <- function(jobj) {
  structure(list(.jobj = jobj), class = c("nlp_light_pipeline", "ml_pipeline_model", "ml_transformer"))
}

#' @export
spark_jobj.nlp_light_pipeline <- function(x, ...) {
  x$.jobj
}
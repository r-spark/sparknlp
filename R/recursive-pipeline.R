#' Spark NLP RecursivePipeline
#' 
#' Recursive pipelines are SparkNLP specific pipelines that allow a Spark ML Pipeline to know about itself on every 
#' Pipeline Stage task, allowing annotators to utilize this same pipeline against external resources to process them 
#' in the same way the user decides. Only some of our annotators take advantage of this. RecursivePipeline behaves
#' exactly the same than normal Spark ML pipelines, so they can be used with the same intention.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/concepts#recursivepipeline}
#'  
#' @param x Either a \code{spark_connection} or \code{ml_pipeline_stage} objects
#' @param uid uid for the pipeline
#' @param ... \code{ml_pipeline_stage} objects
#'  
#' @return When \code{x} is a \code{spark_connection}, \code{ml_pipeline()} returns an empty pipeline object. 
#' When \code{x} is a \code{ml_pipeline_stage}, \code{ml_pipeline()} returns an \code{ml_pipeline} with the stages 
#' set to \code{x} and any transformers or estimators given in \code{...}.
#' @export
nlp_recursive_pipeline <- function(x, ..., uid = random_string("recursive_pipeline_")) {
  UseMethod("nlp_recursive_pipeline")
}

#' @export
nlp_recursive_pipeline.spark_connection <- function(x, ..., uid = random_string("recursive_pipeline_")) {
  uid <- forge::cast_string(uid)
  jobj <- invoke_new(x, "com.johnsnowlabs.nlp.RecursivePipeline", uid)
  
  new_nlp_recursive_pipeline(jobj)
}

#' @export
nlp_recursive_pipeline.ml_pipeline_stage <- function(x, ..., uid = random_string("recursive_pipeline_")) {
  uid <- forge::cast_string(uid)
  sc <- spark_connection(x)
  dots <- list(...) %>%
    lapply(function(x) spark_jobj(x))
  stages <- c(spark_jobj(x), dots)
  jobj <- invoke_static(sc, "sparknlp.Utils", "createRecursivePipelineFromStages", uid, stages)
  new_nlp_recursive_pipeline(jobj)
}


new_nlp_recursive_pipeline <- function(jobj, ..., class = character()) {
  stages <- tryCatch({
    jobj %>%
      invoke("getStages") %>%
      lapply(ml_call_constructor)
  },
  error = function(e) {
    NULL
  })
  
  new_ml_estimator(jobj,
                   stages = stages,
                   stage_uids = if (rlang::is_null(stages))
                     NULL
                   else
                     sapply(stages, function(x)
                       x$uid),
                   ...,
                   class = c(class, "nlp_recursive_pipeline", "ml_pipeline"))
}

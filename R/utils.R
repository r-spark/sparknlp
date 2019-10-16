# Default scala arguments:
# Constructor (e.g. new PretrainedPipeline()):
# model_class <- "com.johnsnowlabs.nlp.pretrained.PretrainedPipeline"
# module <- invoke_static(x, paste0(model_class, "$"), "MODULE$")
# default_lang <- invoke(module, "apply$default$2")
# default_source <- invoke(module, "apply$default$3")

# Static method (e.g. Perceptron.pretrained()):
# module <- invoke_static(sc, paste0(model_class, "$"), "MODULE$")
# default_name <- invoke(module, "pretrained$default$1")
# default_lang <- invoke(module, "pretrained$default$2")
# default_remote_loc <- invoke(module, "pretrained$default$3")

# Get a pretrained model.
# The model_class is the Scala class for the model.
pretrained_model <- function(sc, model_class, name = NULL, lang = NULL, remote_loc = NULL) {
  module <- invoke_static(sc, paste0(model_class, "$"), "MODULE$")
  default_name <- invoke(module, "pretrained$default$1")
  default_lang <- invoke(module, "pretrained$default$2")
  default_remote_loc <- invoke(module, "pretrained$default$3")
  
  if (is.null(name)) name = default_name
  if (is.null(lang)) lang = default_lang
  if (is.null(remote_loc)) remote_loc = default_remote_loc
  
  invoke_static(sc, model_class, "pretrained", name, lang, remote_loc)
}

#' Annotate some text
#' 
#' Use SparkNLP to annotate some text. 
#' 
#' @param x some SparkNLP object that has an annotate method that takes a Spark data frame as argument
#' @param text the text to annotate
#' 
#' @return a Spark data frame containing the annotations
#' 
#' @export
nlp_annotate <- function(x, text) {
  sc <- spark_connection(x)
  text_frame <- dplyr::copy_to(sc, data.frame(text = text))
  
  sdf_register(invoke(spark_jobj(x), "annotate", spark_dataframe(text_frame), "text"))
}
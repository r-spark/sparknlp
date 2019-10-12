#' In most cases you can just leave the parameters NULL (except for the Spark connection) and the Spark NLP defaults
#' will be used.
#' 
#' @param sc A Spark connection
#' @param name the name of the model to load. If NULL will use the default value
#' @param lang the language of the model to be loaded. If NULL will use the default value
#' @param remote_loc the remote location of the model. If NULL will use the default value
#' 
#' @return The Spark NLP model with the pretrained model loaded

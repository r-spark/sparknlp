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
  UseMethod("nlp_annotate", x)
}

#' @export
nlp_annotate.nlp_light_pipeline <- function(x, text) {
  invoke(x$.jobj, "annotateJava", text)
  #invoke_static(spark_connection(x$.jobj), "sparknlp.Utils", "lightPipelineAnnotate", x$.jobj, text)
}

#' @export
nlp_annotate.default <- function(x, text) {
  if (is.character(text)) {
    lp <- nlp_light_pipeline(x)
    return(nlp_annotate(lp, text))
  } else {
    sc <- spark_connection(x)
    text_frame <- dplyr::copy_to(sc, data.frame(text = text))
    
    return(sdf_register(invoke(spark_jobj(x), "annotate", spark_dataframe(text_frame), "text")))
  }
}
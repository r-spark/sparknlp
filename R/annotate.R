#' Annotate some text
#' 
#' Use SparkNLP to annotate some text. 
#' 
#' @param x some SparkNLP object that has an annotate method that takes a Spark data frame as argument
#' @param target the text to annotate. This can be a character string, a character vector or a data frame (with the text
#' in a field named "text")
#' @param column the column name containing text if a Spark DataFrame is passed in.
#' 
#' @return If given a character vector the return value is a list of lists containing the annotations.
#' 
#' If given a Spark DataFrame the return value is a Spark data frame containing the annotations
#' 
#' @export
nlp_annotate <- function(x, target, column = NULL) {
  UseMethod("nlp_annotate", x)
}

#' Fully annotate some text
#' 
#' Use Spark NLP to fully annotate some text.
#' 
#' @param x some SparkNLP object that has an annotate method that takes a Spark data frame as argument
#' @param target the text to annotate. This can be a character string, a character vector or a data frame (with the text
#' in a field named "text")
#' @param column the column name containing text if a Spark DataFrame is passed in.
#' 
#' @return If given a character vector the return value is a list of lists containing the annotations.
#' 
#' If given a Spark DataFrame the return value is a Spark data frame containing the annotations
#' 
#' @export
nlp_annotate_full <- function(x, target, column = NULL) {
  UseMethod("nlp_annotate_full", x)
}

#' @export
nlp_annotate.nlp_light_pipeline <- function(x, target, column = NULL) {
  if (is.character(target)) {
    if (length(target) == 1) {
      return(invoke(x$.jobj, "annotateJava", forge::cast_string(target)))
    } else {
      result <- invoke_static(spark_connection(x$.jobj), "sparknlp.Utils", "annotateList", x$.jobj, forge::cast_string_list(target))
      return(result)
    }
  } else if ("tbl_spark" %in% class(target)) {
    if (is.null(column)) {
      stop("annotate column argument required when targeting a DataFrame")
    }
    return(ml_transform(x, dplyr::rename(target, text = column)))
  } else {
    stop("target must be either a Spark DataFrame, a string or a character vector")
  }
}

#' @export
nlp_annotate.default <- function(x, target, column = NULL) {
  if (is.character(target)) {
    lp <- nlp_light_pipeline(x)
    return(nlp_annotate.nlp_light_pipeline(lp, target, column))
  } else if ("tbl_spark" %in% class(target)) {
    if (is.null(column)) {
      stop("annotate column argument required when targeting a DataFrame")
    }
    return(ml_transform(x, dplyr::rename(target, text = column)))
  } else {
    stop("target must be either a Spark DataFrame, a string or a character vector")
  }
}

#' @export
nlp_annotate_full.nlp_light_pipeline <- function(x, target, column = NULL) {
  if (is.character(target)) {
    if (length(target) == 1) {
      return(invoke(x$.jobj, "fullAnnotateJava", forge::cast_string(target)))
    } else {
      result <- invoke_static(spark_connection(x$.jobj), "sparknlp.Utils", "fullAnnotateList", x$.jobj, forge::cast_string_list(target))
      return(result)
    }
  } else if ("tbl_spark" %in% class(target)) {
    if (is.null(column)) {
      stop("annotate column argument required when targeting a DataFrame")
    }
    return(ml_transform(x, dplyr::rename(target, text = column)))
  } else {
    stop("target must be either a Spark DataFrame, a string or a character vector")
  }
}

#' @export
nlp_annotate_full.default <- function(x, target, column = NULL) {
  if (is.character(target)) {
    lp <- nlp_light_pipeline(x)
    return(nlp_annotate.nlp_light_pipeline(lp, target, column))
  } else if ("tbl_spark" %in% class(target)) {
    if (is.null(column)) {
      stop("annotate column argument required when targeting a DataFrame")
    }
    return(ml_transform(x, dplyr::rename(target, text = column)))
  } else {
    stop("target must be either a Spark DataFrame, a string or a character vector")
  }
}

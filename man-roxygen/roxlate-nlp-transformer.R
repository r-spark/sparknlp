#' @param x A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.
#' @param uid A character string used to uniquely identify the ML transformer.
#' @param ... Optional arguments, see Details.
#'
#' @return The object returned depends on the class of \code{x}.
#'
#' \itemize{
#'   \item \code{spark_connection}: When \code{x} is a \code{spark_connection}, the function returns an instance of a \code{ml_transformer} object. The object contains a pointer to
#'   a Spark \code{Transformer} object and can be used to compose
#'   \code{Pipeline} objects.
#'
#'   \item \code{ml_pipeline}: When \code{x} is a \code{ml_pipeline}, the function returns a \code{ml_pipeline} with
#'   the NLP transformer/annotator appended to the pipeline.
#'
#'   \item \code{tbl_spark}: When \code{x} is a \code{tbl_spark}, a transformer is constructed then
#'   immediately fit with the input \code{tbl_spark}, returning the transformed data frame.
#' }

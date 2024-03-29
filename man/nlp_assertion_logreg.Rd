% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/assertion_logreg.R
\name{nlp_assertion_logreg}
\alias{nlp_assertion_logreg}
\title{Spark NLP AssertionLogRegApproach}
\usage{
nlp_assertion_logreg(
  x,
  input_cols,
  output_col,
  label_column = NULL,
  max_iter = NULL,
  reg = NULL,
  enet = NULL,
  before = NULL,
  after = NULL,
  start_col = NULL,
  end_col = NULL,
  lazy_annotator = NULL,
  uid = random_string("assertion_logreg_")
)
}
\arguments{
\item{x}{A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.}

\item{input_cols}{Input columns. String array.}

\item{output_col}{Output column. String.}

\item{label_column}{Column with one label per document}

\item{max_iter}{Max number of iterations for algorithm}

\item{reg}{Regularization parameter}

\item{enet}{Elastic net parameter}

\item{before}{Amount of tokens from the context before the target}

\item{after}{Amount of tokens from the context after the target}

\item{start_col}{Column that contains the token number for the start of the target}

\item{end_col}{Column that contains the token number for the end of the target}

\item{lazy_annotator}{a Param in Annotators that allows them to stand idle in the Pipeline and do nothing. Can be called by other Annotators in a RecursivePipeline}

\item{uid}{A character string used to uniquely identify the ML estimator.}
}
\value{
The object returned depends on the class of \code{x}.

\itemize{
\item \code{spark_connection}: When \code{x} is a \code{spark_connection}, the function returns an instance of a \code{ml_estimator} object. The object contains a pointer to
a Spark \code{Estimator} object and can be used to compose
\code{Pipeline} objects.

\item \code{ml_pipeline}: When \code{x} is a \code{ml_pipeline}, the function returns a \code{ml_pipeline} with
the NLP estimator appended to the pipeline.

\item \code{tbl_spark}: When \code{x} is a \code{tbl_spark}, an estimator is constructed then
immediately fit with the input \code{tbl_spark}, returning an NLP model.
}
}
\description{
Spark ML estimator that
See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_annotators#assertionlogreg}
}

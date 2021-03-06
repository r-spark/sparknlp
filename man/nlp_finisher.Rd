% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/finisher.R
\name{nlp_finisher}
\alias{nlp_finisher}
\title{Spark NLP Finisher}
\usage{
nlp_finisher(
  x,
  input_cols,
  output_cols = NULL,
  clean_annotations = NULL,
  value_split_symbol = NULL,
  annotation_split_symbol = NULL,
  include_metadata = NULL,
  output_as_array = NULL,
  uid = random_string("finisher_")
)
}
\arguments{
\item{x}{A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.}

\item{input_cols}{Input columns. String array.}

\item{output_cols}{Output columns. String array.}

\item{clean_annotations}{Boolean. Whether to remove intermediate annotations}

\item{value_split_symbol}{String. Optional. Split values within an annotation character}

\item{annotation_split_symbol}{String. Optional. Split values between annotations character}

\item{include_metadata}{Boolean. Optional. Whether to include metadata keys. Sometimes useful in some annotations}

\item{output_as_array}{Boolean. Optional. Whether to output as Array. Useful as input for other Spark transformers}

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
Spark ML transformer that outputs annotation(s) values into string.
See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#finisher}
}

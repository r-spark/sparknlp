% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/relation_extraction_dl.R
\name{nlp_relation_extraction_dl}
\alias{nlp_relation_extraction_dl}
\title{Spark NLP RelationExtractionDLModel}
\usage{
nlp_relation_extraction_dl(
  x,
  input_cols,
  output_col,
  category_names = NULL,
  max_sentence_length = NULL,
  prediction_threshold = NULL,
  uid = random_string("relation_extraction_dl_")
)
}
\arguments{
\item{x}{A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.}

\item{input_cols}{Input columns. String array.}

\item{output_col}{Output column. String.}

\item{category_names}{list of relation names}

\item{max_sentence_length}{Max sentence length to process (Default: 128)}

\item{prediction_threshold}{Minimal activation of the target unit to encode a new relation instance (Default: 0.5f)}

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
Spark ML transformer that extracts and classifies instances of relations
between named entities. In contrast with RelationExtractionModel,
RelationExtractionDLModel is based on BERT. For pretrained models please
see the Models Hub for available models.
}
\details{
See \url{https://nlp.johnsnowlabs.com/licensed/api/com/johnsnowlabs/nlp/annotators/re/RelationExtractionDLModel.html}
}

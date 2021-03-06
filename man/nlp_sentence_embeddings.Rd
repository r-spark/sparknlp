% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sentence-embeddings.R
\name{nlp_sentence_embeddings}
\alias{nlp_sentence_embeddings}
\title{Spark NLP SentenceEmbeddings}
\usage{
nlp_sentence_embeddings(
  x,
  input_cols,
  output_col,
  pooling_strategy = NULL,
  storage_ref = NULL,
  uid = random_string("sentence_embeddings_")
)
}
\arguments{
\item{x}{A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.}

\item{input_cols}{Input columns. String array.}

\item{output_col}{Output column. String.}

\item{pooling_strategy}{Choose how you would like to aggregate Word Embeddings to Sentence Embeddings: AVERAGE or SUM}

\item{storage_ref}{storage reference for the embeddings}

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
Spark ML transformer that converts the results from WordEmbeddings or BertEmbeddings into sentence or document
embeddings by either summing up or averaging all the word embeddings in a sentence or a document
(depending on the input_cols).
See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#sentenceembeddings}
}

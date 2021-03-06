% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/sentence_entity_resolver.R
\name{nlp_sentence_entity_resolver}
\alias{nlp_sentence_entity_resolver}
\title{Spark NLP SentenceEntityResolverApproach}
\usage{
nlp_sentence_entity_resolver(
  x,
  input_cols,
  output_col,
  label_column = NULL,
  normalized_col = NULL,
  neighbors = NULL,
  threshold = NULL,
  miss_as_empty = NULL,
  case_sensitive = NULL,
  confidence_function = NULL,
  distance_function = NULL,
  uid = random_string("sentence_entity_resolver_")
)
}
\arguments{
\item{x}{A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.}

\item{input_cols}{Input columns. String array.}

\item{output_col}{Output column. String.}

\item{label_column}{column name for the value we are trying to resolve}

\item{normalized_col}{column name for the original, normalized description}

\item{neighbors}{number of neighbors to consider in the KNN query to calculate WMD}

\item{threshold}{threshold value for the aggregated distance}

\item{miss_as_empty}{whether or not to return an empty annotation on unmatched chunks}

\item{case_sensitive}{whether the entity should be considered using case sensitivity}

\item{confidence_function}{what function to use to calculate confidence: INVERSE or SOFTMAX}

\item{distance_function}{what distance function to use for KNN: 'EUCLIDEAN' or 'COSINE'}

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
Spark ML estimator that assigns a standard code (ICD10 CM, PCS, ICDO; CPT) to
sentence embeddings pooled over chunks from TextMatchers or the NER Models.
This annotator is particularly handy when working with BertSentenceEmbeddings
from the upstream chunks.
}
\details{
See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_annotators#sentenceentityresolver}
}

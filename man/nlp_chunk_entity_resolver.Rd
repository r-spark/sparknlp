% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/chunk_entity_resolver.R
\name{nlp_chunk_entity_resolver}
\alias{nlp_chunk_entity_resolver}
\title{Spark NLP ChunkEntityResolverApproach}
\usage{
nlp_chunk_entity_resolver(
  x,
  input_cols,
  output_col,
  all_distances_metadata = NULL,
  alternatives = NULL,
  case_sensitive = NULL,
  confidence_function = NULL,
  distance_function = NULL,
  distance_weights = NULL,
  enable_jaccard = NULL,
  enable_jaro_winkler = NULL,
  enable_levenshtein = NULL,
  enable_sorensen_dice = NULL,
  enable_tfidf = NULL,
  enable_wmd = NULL,
  extra_mass_penalty = NULL,
  label_column = NULL,
  miss_as_empty = NULL,
  neighbors = NULL,
  normalized_col = NULL,
  pooling_strategy = NULL,
  threshold = NULL,
  uid = random_string("chunk_entity_resolver_")
)
}
\arguments{
\item{x}{A \code{spark_connection}, \code{ml_pipeline}, or a \code{tbl_spark}.}

\item{input_cols}{Input columns. String array.}

\item{output_col}{Output column. String.}

\item{all_distances_metadata}{whether or not to return an all distance values in the metadata.}

\item{alternatives}{number of results to return in the metadata after sorting by last distance calculated}

\item{case_sensitive}{whether to treat the entities as case sensitive}

\item{confidence_function}{what function to use to calculate confidence: INVERSE or SOFTMAX}

\item{distance_function}{what distance function to use for KNN: 'EUCLIDEAN' or 'COSINE'}

\item{distance_weights}{distance weights to apply before pooling: (WMD, TFIDF, Jaccard, SorensenDice, JaroWinkler, Levenshtein)}

\item{enable_jaccard}{whether or not to use Jaccard token distance.}

\item{enable_jaro_winkler}{whether or not to use Jaro-Winkler character distance.}

\item{enable_levenshtein}{whether or not to use Levenshtein character distance.}

\item{enable_sorensen_dice}{whether or not to use Sorensen-Dice token distance.}

\item{enable_tfidf}{whether or not to use TFIDF token distance.}

\item{enable_wmd}{whether or not to use WMD token distance.}

\item{extra_mass_penalty}{penalty for extra words in the knowledge base match during WMD calculation}

\item{label_column}{column name for the value we are trying to resolve}

\item{miss_as_empty}{whether or not to return an empty annotation on unmatched chunks}

\item{neighbors}{number of neighbours to consider in the KNN query to calculate WMD}

\item{normalized_col}{column name for the original, normalized description}

\item{pooling_strategy}{pooling strategy to aggregate distances: AVERAGE or SUM}

\item{threshold}{threshold value for the aggregated distance}

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
See \url{https://nlp.johnsnowlabs.com/docs/en/licensed_annotators#chunkentityresolver}
}

% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/chunk_entity_resolver.R
\name{nlp_chunk_entity_resolver_pretrained}
\alias{nlp_chunk_entity_resolver_pretrained}
\title{Load a pretrained Spark NLP Chunk Entity Resolver model}
\usage{
nlp_chunk_entity_resolver_pretrained(
  sc,
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
  miss_as_empty = NULL,
  neighbors = NULL,
  pooling_strategy = NULL,
  threshold = NULL,
  name,
  lang = NULL,
  remote_loc = NULL
)
}
\arguments{
\item{sc}{A Spark connection}

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

\item{miss_as_empty}{whether or not to return an empty annotation on unmatched chunks}

\item{neighbors}{number of neighbours to consider in the KNN query to calculate WMD}

\item{pooling_strategy}{pooling strategy to aggregate distances: AVERAGE or SUM}

\item{threshold}{threshold value for the aggregated distance#'}

\item{name}{the name of the model to load. If NULL will use the default value}

\item{lang}{the language of the model to be loaded. If NULL will use the default value}

\item{remote_loc}{the remote location of the model. If NULL will use the default value}
}
\value{
The Spark NLP model with the pretrained model loaded
}
\description{
Create a pretrained Spark NLP \code{ChunkEntityResolverModel} model
}

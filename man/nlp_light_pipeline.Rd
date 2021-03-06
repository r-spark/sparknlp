% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/light-pipeline.R
\name{nlp_light_pipeline}
\alias{nlp_light_pipeline}
\title{Spark NLP Light pipeline}
\usage{
nlp_light_pipeline(x, parse_embeddings = FALSE)
}
\arguments{
\item{x}{a trained (fitted) pipeline}

\item{parse_embeddings}{whether to parse the embeddings}
}
\value{
a LightPipeline object
}
\description{
LightPipelines are Spark ML pipelines converted into a single machine but multithreaded task, becoming more than
10x times faster for smaller amounts of data (small is relative, but 50k sentences is roughly a good maximum).
To use them, simply plug in a trained (fitted) pipeline.
}

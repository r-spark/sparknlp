% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/roberta_sentence_embeddings.R
\name{nlp_roberta_sentence_embeddings_pretrained}
\alias{nlp_roberta_sentence_embeddings_pretrained}
\title{Load a pretrained Spark NLP RoBertaSentenceEmbeddings model}
\usage{
nlp_roberta_sentence_embeddings_pretrained(
  sc,
  input_cols,
  output_col,
  case_sensitive = NULL,
  batch_size = NULL,
  dimension = NULL,
  max_sentence_length = NULL,
  name = NULL,
  lang = NULL,
  remote_loc = NULL
)
}
\arguments{
\item{sc}{A Spark connection}

\item{input_cols}{Input columns. String array.}

\item{output_col}{Output column. String.}

\item{case_sensitive}{whether to lowercase tokens or not}

\item{batch_size}{batch size}

\item{dimension}{defines the output layer of BERT when calculating embeddings}

\item{max_sentence_length}{max sentence length to process}

\item{name}{the name of the model to load. If NULL will use the default value}

\item{lang}{the language of the model to be loaded. If NULL will use the default value}

\item{remote_loc}{the remote location of the model. If NULL will use the default value}
}
\value{
The Spark NLP model with the pretrained model loaded
}
\description{
Create a pretrained Spark NLP \code{RoBertaSentenceEmbeddings} model.
Sentence-level embeddings using RoBERTa. The RoBERTa model was proposed in
RoBERTa: A Robustly Optimized BERT Pretraining Approach by Yinhan Liu, Myle Ott,
Naman Goyal, Jingfei Du, Mandar Joshi, Danqi Chen, Omer Levy, Mike Lewis,
Luke Zettlemoyer, Veselin Stoyanov. It is based on Google's BERT model
released in 2018.
}
\details{
It builds on BERT and modifies key hyperparameters, removing the next-sentence pretraining objective and training with much larger mini-batches and learning rates.
See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#robertabertsentenceembeddings}
}

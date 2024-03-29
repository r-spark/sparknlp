% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/bert_sentence_chunk_embeddings.R
\name{nlp_bert_sentence_chunk_embeddings_pretrained}
\alias{nlp_bert_sentence_chunk_embeddings_pretrained}
\title{Load a pretrained Spark NLP BertSentenceChunkEmbeddings model}
\usage{
nlp_bert_sentence_chunk_embeddings_pretrained(
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
Create a pretrained Spark NLP \code{BertSentenceChunkEmbeddings} model.
BERT Sentence embeddings for chunk annotations which take into account the context
of the sentence the chunk appeared in. This is an extension of BertSentenceEmbeddings
which combines the embedding of a chunk with the embedding of the surrounding sentence.
For each input chunk annotation, it finds the corresponding sentence, computes the BERT
sentence embedding of both the chunk and the sentence and averages them. The resulting
embeddings are useful in cases, in which one needs a numerical representation of a text
chunk which is sensitive to the context it appears in.
}
\details{
This model is a subclass of BertSentenceEmbeddings and shares all parameters with it. It can
load any pretrained BertSentenceEmbeddings model.
See \url{https://nlp.johnsnowlabs.com/licensed/api/com/johnsnowlabs/nlp/annotators/embeddings/BertSentenceChunkEmbeddings.html}
}

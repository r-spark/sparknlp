#' Spark NLP NGramGenerator
#'
#' Spark ML transformer that takes as input a sequence of strings (e.g. the output of a Tokenizer, Normalizer, Stemmer,
#'  Lemmatizer, and StopWordsCleaner). The parameter n is used to determine the number of terms in each n-gram. 
#'  The output will consist of a sequence of n-grams where each n-gram is represented by a space-delimited string of n 
#'  consecutive words with annotatorType CHUNK same as the Chunker annotator.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#ngramgenerator}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param n number elements per n-gram (>=1)
#' @param enable_cumulative whether to calculate just the actual n-grams or all n-grams from 1 through n
#' @param delimter glue character used to join the tokens
#' 
#' @export
nlp_ngram_generator <- function(x, input_cols, output_col,
                 n = NULL, enable_cumulative = NULL, delimiter = NULL,
                 uid = random_string("ngram_generator_")) {
  UseMethod("nlp_ngram_generator")
}

#' @export
nlp_ngram_generator.spark_connection <- function(x, input_cols, output_col,
                 n = NULL, enable_cumulative = NULL, delimiter = NULL,
                 uid = random_string("ngram_generator_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    n = n,
    enable_cumulative = enable_cumulative,
    delimiter = delimiter,
    uid = uid
  ) %>%
  validator_nlp_ngram_generator()

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.NGramGenerator",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setN", args[["n"]])  %>%
    sparklyr::jobj_set_param("setEnableCumulative", args[["enable_cumulative"]]) %>% 
    sparklyr::jobj_set_param("setDelimiter", args[["delimiter"]])

  new_nlp_ngram_generator(jobj)
}

#' @export
nlp_ngram_generator.ml_pipeline <- function(x, input_cols, output_col,
                 n = NULL, enable_cumulative = NULL, delimiter = NULL,
                 uid = random_string("ngram_generator_")) {

  stage <- nlp_ngram_generator.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    n = n,
    enable_cumulative = enable_cumulative,
    delimiter = delimiter,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_ngram_generator.tbl_spark <- function(x, input_cols, output_col,
                 n = NULL, enable_cumulative = NULL, delimiter = NULL,
                 uid = random_string("ngram_generator_")) {
  stage <- nlp_ngram_generator.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    n = n,
    enable_cumulative = enable_cumulative,
    delimiter = delimiter,
    uid = uid
  )

  stage %>% sparklyr::ml_transform(x)
}
#' @import forge
validator_nlp_ngram_generator <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["n"]] <- cast_nullable_integer(args[["n"]])
  args[["enable_cumulative"]] <- cast_nullable_logical(args[["enable_cumulative"]])
  args[["delimiter"]] <- cast_nullable_string(args[["delimiter"]])
  args
}

new_nlp_ngram_generator <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_ngram_generator")
}

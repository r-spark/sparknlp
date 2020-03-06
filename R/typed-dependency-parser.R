#' Spark NLP TypedDependencyParserApproach
#'
#' Spark ML estimator that is a labeled parser that finds a grammatical relation between two words in a sentence. 
#' Its input is a CoNLL2009 or ConllU dataset.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#typed-dependency-parser}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param n_iterations 
#' @param conll_u_path path to a file in CoNLL-U format
#' @param conll_u_read_as TEXT or SPARK_DATASET
#' @param conll_u_options options to pass to the Spark reader
#' @param conll_2009_path path to a file in CoNLL 2009 format
#' @param conll_2009_read_as TEXT or SPARK_DATASET
#' @param conll_2009_options options to pass to the Spark reader
#' 
#' @export
nlp_typed_dependency_parser <- function(x, input_cols, output_col,
                 n_iterations = NULL, conll_u_path = NULL, conll_u_read_as = "TEXT", conll_u_options = list("format" = "text"), 
                 conll_2009_path = NULL, conll_2009_read_as = "TEXT", conll_2009_options = list("format" = "text"),
                 uid = random_string("typed_dependency_parser_")) {
  UseMethod("nlp_typed_dependency_parser")
}

#' @export
nlp_typed_dependency_parser.spark_connection <- function(x, input_cols, output_col,
                                                         n_iterations = NULL, conll_u_path = NULL, 
                                                         conll_u_read_as = "TEXT", 
                                                         conll_u_options = list("format" = "text"), 
                                                         conll_2009_path = NULL, conll_2009_read_as = "TEXT", 
                                                         conll_2009_options = list("format" = "text"),
                                                         uid = random_string("typed_dependency_parser_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    n_iterations = n_iterations,
    conll_u_path = conll_u_path,
    conll_u_read_as = conll_u_read_as,
    conll_u_options = conll_u_options,
    conll_2009_path = conll_2009_path,
    conll_2009_read_as = conll_2009_read_as,
    conll_2009_options = conll_2009_options,
    uid = uid
  ) %>%
  validator_nlp_typed_dependency_parser()
  
  if (!is.null(args[["conll_u_options"]])) {
    args[["conll_u_options"]] = list2env(args[["conll_u_options"]])
  }
  
  if (!is.null(args[["conll_2009_options"]])) {
    args[["conll_2009_options"]] = list2env(args[["conll_2009_options"]])
  }

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.parser.typdep.TypedDependencyParserApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setNumberOfIterations", args[["n_iterations"]])
  
  if (!is.null(args[["conll_u_path"]])) {
    sparklyr::invoke(jobj, "setConllU", args[["conll_u_path"]], 
                     read_as(x, args[["conll_u_read_as"]]), args[["conll_u_options"]])
  }
  
  if (!is.null(args[["conll_2009_path"]])) {
    sparklyr::invoke(jobj, "setConll2009", args[["conll_2009_path"]], 
                     read_as(x, args[["conll_2009_read_as"]]), args[["conll_2009_options"]])
  }

  new_nlp_typed_dependency_parser(jobj)
}

#' @export
nlp_typed_dependency_parser.ml_pipeline <- function(x, input_cols, output_col, 
                                                    n_iterations = NULL, conll_u_path = NULL, 
                                                    conll_u_read_as = "TEXT", 
                                                    conll_u_options = list("format" = "text"), 
                                                    conll_2009_path = NULL, conll_2009_read_as = "TEXT", 
                                                    conll_2009_options = list("format" = "text"),
                                                    uid = random_string("typed_dependency_parser_")) {

  stage <- nlp_typed_dependency_parser.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    n_iterations = n_iterations,
    conll_u_path = conll_u_path,
    conll_u_read_as = conll_u_read_as,
    conll_u_options = conll_u_options,
    conll_2009_path = conll_2009_path,
    conll_2009_read_as = conll_2009_read_as,
    conll_2009_options = conll_2009_options,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_typed_dependency_parser.tbl_spark <- function(x, input_cols, output_col,
                                                  n_iterations = NULL, conll_u_path = NULL, 
                                                  conll_u_read_as = "TEXT", 
                                                  conll_u_options = list("format" = "text"), 
                                                  conll_2009_path = NULL, conll_2009_read_as = "TEXT", 
                                                  conll_2009_options = list("format" = "text"),
                                                  uid = random_string("typed_dependency_parser_")) {
  stage <- nlp_typed_dependency_parser.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    n_iterations = n_iterations,
    conll_u_path = conll_u_path,
    conll_u_read_as = conll_u_read_as,
    conll_u_options = conll_u_options,
    conll_2009_path = conll_2009_path,
    conll_2009_read_as = conll_2009_read_as,
    conll_2009_options = conll_2009_options,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}

#' Load a pretrained Spark NLP model
#' 
#' Create a pretrained Spark NLP \code{TypedDependencyParserModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @export
nlp_typed_dependency_parser_pretrained <- function(sc, input_cols, output_col,
                                             name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  ) %>%
    validator_nlp_dependency_parser()
  
  model_class <- "com.johnsnowlabs.nlp.annotators.parser.typdep.TypedDependencyParserModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])
  
  new_ml_transformer(model)
}

#' @import forge
validator_nlp_typed_dependency_parser <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["n_iterations"]] <- cast_nullable_integer(args[["n_iterations"]])
  args[["conll_u_path"]] <- cast_nullable_string(args[["conll_u_path"]])
  args[["conll_u_read_as"]] <- cast_choice(args[["conll_u_read_as"]], choices = c("TEXT", "SPARK_DATASET"), allow_null = TRUE)
  args[["conll_2009_path"]] <- cast_nullable_string(args[["conll_2009_path"]])
  args[["conll_2009_read_as"]] <- cast_choice(args[["conll_2009_read_as"]], choices = c("TEXT", "SPARK_DATASET"), allow_null = TRUE)
  args
}

new_nlp_typed_dependency_parser <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_typed_dependency_parser")
}



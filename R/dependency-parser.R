#' Spark NLP DependencyParserApproach
#'
#' Spark ML estimator unlabeled parser that finds a grammatical relation between two words in a sentence.
#'  Its input is a directory with dependency treebank files.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#dependency-parser}
#' 
#' @template roxlate-nlp-algo
#' @template roxlate-inputs-output-params
#' @param n_iterations Number of iterations in training, converges to better accuracy
#' @param tree_bank_path Dependency treebank folder with files in Penn Treebank format
#' @param tree_bank_read_as TEXT or SPARK_DATASET
#' @param tree_bank_options options to pass to Spark reader
#' @param conll_u_path Path to a file in CoNLL-U format
#' @param conll_u_read_as TEXT or SPARK_DATASET
#' @param conll_u_options options to pass to Spark reader
#' 
#' @export
nlp_dependency_parser <- function(x, input_cols, output_col,
                 n_iterations = NULL, tree_bank_path = NULL, tree_bank_read_as = "TEXT", 
                 tree_bank_options = list("format" = "text"), conll_u_path = NULL, conll_u_read_as = "TEXT",
                 conll_u_options = list("format" = "text"),
                 uid = random_string("dependency_parser_")) {
  UseMethod("nlp_dependency_parser")
}

#' @export
nlp_dependency_parser.spark_connection <- function(x, input_cols, output_col,
                                                   n_iterations = NULL, tree_bank_path = NULL, tree_bank_read_as = "TEXT", 
                                                   tree_bank_options = list("format" = "text"), conll_u_path = NULL, conll_u_read_as = "TEXT",
                                                   conll_u_options = list("format" = "text"),
                                                   uid = random_string("dependency_parser_")) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col,
    n_iterations = n_iterations,
    tree_bank_path = tree_bank_path,
    tree_bank_read_as = tree_bank_read_as,
    tree_bank_options = tree_bank_options,
    conll_u_path = conll_u_path,
    conll_u_read_as = conll_u_read_as,
    conll_u_options = conll_u_options,
    uid = uid
  ) %>%
  validator_nlp_dependency_parser()
  
  if (!is.null(args[["tree_bank_options"]])) {
    args[["tree_bank_options"]] = list2env(args[["tree_bank_options"]])
  }
  
  if (!is.null(args[["conll_u_options"]])) {
    args[["conll_u_options"]] = list2env(args[["conll_u_options"]])
  }

  jobj <- sparklyr::spark_pipeline_stage(
    x, "com.johnsnowlabs.nlp.annotators.parser.dep.DependencyParserApproach",
    input_cols = args[["input_cols"]],
    output_col = args[["output_col"]],
    uid = args[["uid"]]
  ) %>%
    sparklyr::jobj_set_param("setNumberOfIterations", args[["n_iterations"]])

  if (!is.null(args[["tree_bank_path"]])) {
    sparklyr::invoke(jobj, "setDependencyTreeBank", args[["tree_bank_path"]], 
                     read_as(x, args[["tree_bank_read_as"]]), args[["tree_bank_options"]])
  }
  
  if (!is.null(args[["conll_u_path"]])) {
    sparklyr::invoke(jobj, "setConllU", args[["conll_u_path"]], 
                     read_as(x, args[["conll_u_read_as"]]), args[["conll_u_options"]])
  }
  
  new_nlp_dependency_parser(jobj)
}

#' @export
nlp_dependency_parser.ml_pipeline <- function(x, input_cols, output_col,
                                              n_iterations = NULL, tree_bank_path = NULL, tree_bank_read_as = "TEXT", 
                                              tree_bank_options = list("format" = "text"), conll_u_path = NULL, conll_u_read_as = "TEXT",
                                              conll_u_options = list("format" = "text"),
                                              uid = random_string("dependency_parser_")) {

  stage <- nlp_dependency_parser.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    n_iterations = n_iterations,
    tree_bank_path = tree_bank_path,
    tree_bank_read_as = tree_bank_read_as,
    tree_bank_options = tree_bank_options,
    conll_u_path = conll_u_path,
    conll_u_read_as = conll_u_read_as,
    conll_u_options = conll_u_options,
    uid = uid
  )

  sparklyr::ml_add_stage(x, stage)
}

#' @export
nlp_dependency_parser.tbl_spark <- function(x, input_cols, output_col,
                                            n_iterations = NULL, tree_bank_path = NULL, tree_bank_read_as = "TEXT", 
                                            tree_bank_options = list("format" = "text"), conll_u_path = NULL, conll_u_read_as = "TEXT",
                                            conll_u_options = list("format" = "text"),
                                            uid = random_string("dependency_parser_")) {
  stage <- nlp_dependency_parser.spark_connection(
    x = sparklyr::spark_connection(x),
    input_cols = input_cols,
    output_col = output_col,
    n_iterations = n_iterations,
    tree_bank_path = tree_bank_path,
    tree_bank_read_as = tree_bank_read_as,
    tree_bank_options = tree_bank_options,
    conll_u_path = conll_u_path,
    conll_u_read_as = conll_u_read_as,
    conll_u_options = conll_u_options,
    uid = uid
  )

  stage %>% sparklyr::ml_fit_and_transform(x)
}

#' Load a pretrained Spark NLP model
#' 
#' Create a pretrained Spark NLP \code{DependencyParserModel} model
#' 
#' @template roxlate-pretrained-params
#' @template roxlate-inputs-output-params
#' @export
nlp_dependency_parser_pretrained <- function(sc, input_cols, output_col,
                                                name = NULL, lang = NULL, remote_loc = NULL) {
  args <- list(
    input_cols = input_cols,
    output_col = output_col
  ) %>%
    validator_nlp_dependency_parser()
  
  model_class <- "com.johnsnowlabs.nlp.annotators.parser.dep.DependencyParserModel"
  model <- pretrained_model(sc, model_class, name, lang, remote_loc)
  spark_jobj(model) %>%
    sparklyr::jobj_set_param("setInputCols", args[["input_cols"]]) %>% 
    sparklyr::jobj_set_param("setOutputCol", args[["output_col"]])
  
  new_ml_transformer(model, class = "nlp_dependency_parser_model")
}

#' @import forge
validator_nlp_dependency_parser <- function(args) {
  args[["input_cols"]] <- cast_string_list(args[["input_cols"]])
  args[["output_col"]] <- cast_string(args[["output_col"]])
  args[["n_iterations"]] <- cast_nullable_integer(args[["n_iterations"]])
  args[["tree_bank_path"]] <- cast_nullable_string(args[["tree_bank_path"]])
  args[["tree_bank_read_as"]] <- cast_choice(args[["tree_bank_read_as"]], choices = c("TEXT", "SPARK_DATASET"), allow_null = TRUE)
  args[["conll_u_path"]] <- cast_nullable_string(args[["conll_u_path"]])
  args[["conll_u_read_as"]] <- cast_choice(args[["conll_u_read_as"]], choices = c("TEXT", "SPARK_DATASET"), allow_null = TRUE)  
  args
}

new_nlp_dependency_parser <- function(jobj) {
  sparklyr::new_ml_estimator(jobj, class = "nlp_dependency_parser")
}

new_nlp_dependency_parser_model <- function(jobj) {
  sparklyr::new_ml_transformer(jobj, class = "nlp_dependency_parser_model")
}

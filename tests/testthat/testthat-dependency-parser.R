setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  pos <- nlp_perceptron_pretrained(sc, input_cols = c("sentence", "token"), output_col = "pos")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, pos)
  test_data <- ml_fit_and_transform(pipeline, text_tbl)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
})

teardown({
  spark_disconnect(sc)
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("dependency_parser param setting", {
  test_args <- list(
    input_cols = c("string1", "string2", "string3"),
    output_col = "string1",
    n_iterations = 100
  )

  test_param_setting(sc, nlp_dependency_parser, test_args)
})

test_that("nlp_dependency_parser spark_connection", {
  test_annotator <- nlp_dependency_parser(sc, input_cols = c("sentence", "pos", "token"), output_col = "dependency",
                                          tree_bank_path = here::here("tests", "testthat", "data", "dependency_treebank"))
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("dependency" %in% colnames(transformed_data))
  
  expect_true(inherits(test_annotator, "nlp_dependency_parser"))
  expect_true(inherits(fit_model, "nlp_dependency_parser_model"))
})

test_that("nlp_dependency_parser ml_pipeline", {
  test_annotator <- nlp_dependency_parser(pipeline, input_cols = c("sentence", "pos", "token"), output_col = "dependency",
                                          tree_bank_path = here::here("tests", "testthat", "data", "dependency_treebank"))
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("dependency" %in% colnames(transformed_data))
})

test_that("nlp_dependency_parser tbl_spark", {
  transformed_data <- nlp_dependency_parser(test_data, input_cols = c("sentence", "pos", "token"), output_col = "dependency",
                                            tree_bank_path = here::here("tests", "testthat", "data", "dependency_treebank"))
  expect_true("dependency" %in% colnames(transformed_data))
})

test_that("nlp_dependency_parser pretrained", {
  model <- nlp_dependency_parser_pretrained(sc, input_cols = c("sentence", "pos", "token"), output_col = "dependency")
  transformed_data <- ml_transform(model, test_data)
  expect_true("dependency" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_dependency_parser_model"))
})

setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "documents")

  pipeline <- ml_pipeline(assembler)
  test_data <- ml_fit_and_transform(pipeline, text_tbl)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("t5_transformer param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    task = "string1",
    max_output_length = 100
  )

  test_param_setting(sc, nlp_t5_transformer, test_args)
})

test_that("nlp_t5_transformer spark_connection", {
  test_annotator <- nlp_t5_transformer(sc, input_cols = c("documents"), output_col = "summaries")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("summaries" %in% colnames(transformed_data))
  
  expect_true(inherits(test_annotator, "nlp_t5_transformer"))
})

test_that("nlp_t5_transformer ml_pipeline", {
  test_annotator <- nlp_t5_transformer(pipeline, input_cols = c("documents"), output_col = "summaries")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("summaries" %in% colnames(transformed_data))
})

test_that("nlp_t5_transformer tbl_spark", {
  transformed_data <- nlp_t5_transformer(test_data, input_cols = c("documents"), output_col = "summaries")
  expect_true("summaries" %in% colnames(transformed_data))
})

test_that("nlp_t5_transformer pretrained", {
  model <- nlp_t5_transformer_pretrained(sc, input_cols = c("documents"), output_col = "summaries",
                                         name = "t5_small")
  transformed_data <- ml_transform(model, test_data)
  expect_true("summaries" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_t5_transformer"))
})


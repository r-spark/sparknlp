setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")

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

test_that("drug_normalizer param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    lower_case = TRUE,
    policy = "string1"
  )

  test_param_setting(sc, nlp_drug_normalizer, test_args)
})

test_that("nlp_drug_normalizer spark_connection", {
  test_annotator <- nlp_drug_normalizer(sc, input_cols = c("document"), output_col = "document_normalized")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("document_normalized" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_drug_normalizer"))
})
  
test_that("nlp_drug_normalizer ml_pipeline", {
  test_annotator <- nlp_drug_normalizer(pipeline, input_cols = c("document"), output_col = "document_normalized")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("document_normalized" %in% colnames(transformed_data))
})

test_that("nlp_drug_normalizer tbl_spark", {
  transformed_data <- nlp_drug_normalizer(test_data, input_cols = c("document"), output_col = "document_normalized")
  expect_true("document_normalized" %in% colnames(transformed_data))
})


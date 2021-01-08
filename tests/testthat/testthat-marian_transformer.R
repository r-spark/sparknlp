setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentences")

  pipeline <- ml_pipeline(assembler, sentdetect)
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

test_that("marian_transformer param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    lang_id = "string1",
    max_input_length = 30,
    max_output_length = 30,
    vocabulary = c("string1", "string2")
  )

  test_param_setting(sc, nlp_marian_transformer, test_args)
})

test_that("nlp_marian_transformer spark_connection", {
  test_annotator <- nlp_marian_transformer(sc, input_cols = c("sentences"), output_col = "translation")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("translation" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_marian_transformer"))
})

test_that("nlp_marian_transformer ml_pipeline", {
  test_annotator <- nlp_marian_transformer(pipeline, input_cols = c("sentences"), output_col = "translation")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("translation" %in% colnames(transformed_data))
})

test_that("nlp_marian_transformer tbl_spark", {
  transformed_data <- nlp_marian_transformer(test_data, input_cols = c("sentences"), output_col = "translation")
  expect_true("translation" %in% colnames(transformed_data))
})

test_that("nlp_marian_transformer pretrained", {
  model <- nlp_marian_transformer_pretrained(sc, input_cols = c("sentences"), output_col = "translation")
  transformed_data <- ml_transform(model, test_data)
  expect_true("translation" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_marian_transformer"))
})


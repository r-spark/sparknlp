setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer)
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

test_that("symmetric_delete param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    max_edit_distance = 2,
    dups_limit = 3,
    deletes_threshold = 2,
    frequency_threshold = 4,
    longest_word_length = 10,
    max_frequency = 15,
    min_frequency = 2
  )

  test_param_setting(sc, nlp_symmetric_delete, test_args)
})

test_that("nlp_symmetric_delete spark_connection", {
  test_annotator <- nlp_symmetric_delete(sc, input_cols = c("token"), output_col = "spell")
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("spell" %in% colnames(transformed_data))
  
  expect_true(inherits(test_annotator, "nlp_symmetric_delete"))
  expect_true(inherits(fit_model, "nlp_symmetric_delete_model"))
})

test_that("nlp_symmetric_delete ml_pipeline", {
  test_annotator <- nlp_symmetric_delete(pipeline, input_cols = c("token"), output_col = "spell")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("spell" %in% colnames(transformed_data))
})

test_that("nlp_symmetric_delete tbl_spark", {
  transformed_data <- nlp_symmetric_delete(test_data, input_cols = c("token"), output_col = "spell")
  expect_true("spell" %in% colnames(transformed_data))
})

test_that("nlp_symmetric_delete pretrained", {
  model <- nlp_symmetric_delete_pretrained(sc, input_cols = c("token"), output_col = "spell")
  transformed_data <- ml_transform(model, test_data)
  expect_true("spell" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_symmetric_delete_model"))
})


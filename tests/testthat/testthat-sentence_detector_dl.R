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

test_that("sentence_detector_dl param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    epochs_number = 5,
    impossible_penultimates = c("string1", "string2"),
    model = "string1",
    output_logs_path = "string1",
    validation_split = 0.8
  )

  test_param_setting(sc, nlp_sentence_detector_dl, test_args)
})

test_that("nlp_sentence_detector_dl spark_connection", {
  test_annotator <- nlp_sentence_detector_dl(sc, input_cols = c("document"), output_col = "sentence")
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("sentence" %in% colnames(transformed_data))
  
  # Test Float parameters
  oldvalue <- ml_param(test_annotator, "validation_split")
  newmodel <- nlp_set_param(test_annotator, "validation_split", 0.8)
  newvalue <- ml_param(newmodel, "validation_split")

  expect_true(inherits(test_annotator, "nlp_sentence_detector_dl"))
  expect_true(inherits(fit_model, "nlp_sentence_detector_dl_model"))
})

test_that("nlp_sentence_detector_dl ml_pipeline", {
  test_annotator <- nlp_sentence_detector_dl(pipeline, input_cols = c("document"), output_col = "sentence")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("sentence" %in% colnames(transformed_data))
})

test_that("nlp_sentence_detector_dl tbl_spark", {
  transformed_data <- nlp_sentence_detector_dl(test_data, input_cols = c("document"), output_col = "sentence")
  expect_true("sentence" %in% colnames(transformed_data))
})

test_that("nlp_sentence_detector_dl pretrained", {
  model <- nlp_sentence_detector_dl_pretrained(sc, input_cols = c("document"), output_col = "sentence")
  transformed_data <- ml_transform(model, test_data)
  expect_true("sentence" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_sentence_detector_dl_model"))
})


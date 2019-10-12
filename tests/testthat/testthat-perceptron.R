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
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("nlp_perceptron spark_connection", {
  test_annotator <- nlp_perceptron(sc, input_cols = c("token", "sentence"), output_col = "pos")
  print(class(test_annotator))
  pos_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(pos_model, test_data)
  expect_true("pos" %in% colnames(transformed_data))
})

# test_that("nlp_perceptron ml_pipeline", {
#   test_annotator <- nlp_perceptron(pipeline, input_cols = c("token", "sentence"), output_col = "pos")
#   transformed_data <- ml_fit_and_transform(test_annotator, test_data)
#   expect_true("pos" %in% colnames(transformed_data))
# })
# 
# test_that("nlp_perceptron tbl_spark", {
#   transformed_data <- nlp_perceptron(test_data, input_cols = c("token", "sentence"), output_col = "pos")
#   expect_true("pos" %in% colnames(transformed_data))
# })


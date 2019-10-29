setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer)
  test_data <- ml_fit_and_transform(pipeline, text_tbl)
  
  pos_dataset <- nlp_pos(sc, here::here("tests", "testthat", "data", "pos_corpus.txt"))

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
  assign("pos_dataset", pos_dataset, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
  rm(pos_dataset, envir = .GlobalEnv)
})

test_that("nlp_perceptron param setting", {
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    n_iterations = 1,
    pos_column = "string1"
  )
  
  test_param_setting(sc, nlp_perceptron, test_args)
})

test_that("nlp_perceptron spark_connection", {
 test_annotator <- nlp_perceptron(sc, input_cols = c("token", "sentence"), output_col = "pos")
 pos_model <- ml_fit(test_annotator, pos_dataset)
 transformed_data <- ml_transform(pos_model, test_data)
 expect_true("pos" %in% colnames(transformed_data))
})

test_that("nlp_perceptron ml_pipeline", {
   test_annotator <- nlp_perceptron(pipeline, input_cols = c("token", "sentence"), output_col = "pos")
   pos_model <- ml_fit(test_annotator, pos_dataset)
   transformed_data <- ml_transform(pos_model, test_data)
   expect_true("pos" %in% colnames(transformed_data))
})

test_that("nlp_perceptron tbl_spark", {
  pos_model <- nlp_perceptron(pos_dataset, input_cols = c("token", "sentence"), output_col = "pos")
  transformed_data <- ml_transform(pos_model, test_data)
  expect_true("pos" %in% colnames(transformed_data))
})


test_that("nlp_perceptron pretrained model", {
  model <- nlp_perceptron_pretrained(sc, input_cols = c("token", "sentence"), output_col = "pos")
  pipeline <- ml_add_stage(pipeline, model)
  transformed_data <- ml_fit_and_transform(pipeline, test_data)
  expect_true("pos" %in% colnames(transformed_data))
})

test_that("nlp_pos read pos training dataset", {
  expect_true("tags" %in% colnames(pos_dataset))
})

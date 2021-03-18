setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")
  
  training_tbl <- sparklyr::spark_read_csv(sc, here::here("tests", "testthat", "data", "i2b2_assertion_sample.csv"))
  

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  doc2chunk <- nlp_doc2chunk(sc, input_cols = c("document"), output_col = "chunk",
                             chunk_col = "target", start_col = "start", start_col_by_token_index = TRUE,
                             fail_on_missing = FALSE, lowercase = TRUE)
  tokenizer <- nlp_tokenizer(sc, input_cols = c("document"), output_col = "token")
  
  embeddings <- nlp_word_embeddings_pretrained(sc, input_cols = c("document", "token"), output_col = "embeddings",
                                               name = "embeddings_clinical", remote_loc = "clinical/models")
 
  pipeline <- ml_pipeline(assembler, doc2chunk, tokenizer, embeddings)
  training_data <- ml_fit_and_transform(pipeline, training_tbl)
  
  
  
  test_data <- ml_fit_and_transform(pipeline, training_tbl)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("training_data", training_data, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(training_data, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("assertion_logreg param setting", {
  test_args <- list(
    input_cols = c("string1", "string2", "string3"),
    output_col = "string1",
    #label_col = "string1",
    max_iter = 5,
    #reg = 0.01,
    #enet = 0.001,
    #before = 3,
    #after = 3,
    start_col = "string1",
    end_col = "string1",
    lazy_annotator = TRUE
  )

  test_param_setting(sc, nlp_assertion_logreg, test_args)
})

test_that("nlp_assertion_logreg spark_connection", {
  test_annotator <- nlp_assertion_logreg(sc, input_cols = c("document","chunk","embeddings"), output_col = "pos",
                                         label_column = "label", max_iter = 26, reg = 0.00192, enet = 0.9, before = 10,
                                         after = 10, start_col = "start", end_col = "end")
  fit_model <- ml_fit(test_annotator, training_data)
  transformed_data <- ml_transform(fit_model, training_data)
  expect_true("pos" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_assertion_logreg"))
  expect_true(inherits(fit_model, "nlp_assertion_logreg_model"))
})

test_that("nlp_assertion_logreg ml_pipeline", {
  test_annotator <- nlp_assertion_logreg(pipeline, input_cols = c("document","chunk","embeddings"), output_col = "pos")
  transformed_data <- ml_fit_and_transform(test_annotator, training_data)
  expect_true("pos" %in% colnames(transformed_data))
})

test_that("nlp_assertion_logreg tbl_spark", {
  transformed_data <- nlp_assertion_logreg(training_data, input_cols = c("document","chunk","embeddings"), output_col = "pos")
  expect_true("pos" %in% colnames(transformed_data))
})

test_that("nlp_assertion_logreg pretrained", {
  model <- nlp_assertion_logreg_pretrained(sc, input_cols = c("document", "chunk", "embeddings"), output_col = "assertion",
                                       name = "assertion_ml", remote_loc = "clinical/models")
  transformed_data <- ml_transform(model, test_data)
  expect_true("assertion" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_assertion_logreg_model"))
})


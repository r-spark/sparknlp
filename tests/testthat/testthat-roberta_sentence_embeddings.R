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
  spark_disconnect(sc)
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("nlp_roberta_sentence_embeddings pretrained", {
  model <- nlp_roberta_sentence_embeddings_pretrained(sc, input_cols = c("document"), output_col = "roberta_sentence_embeddings")
  transformed_data <- ml_transform(model, test_data)
  expect_true("roberta_sentence_embeddings" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_roberta_sentence_embeddings"))
})


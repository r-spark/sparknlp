setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

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

test_that("nlp_elmo_embeddings pretrained", {
  model <- nlp_elmo_embeddings_pretrained(sc, input_cols = c("sentence", "token"), output_col = "elmo")
  transformed_data <- ml_transform(model, test_data)
  expect_true("elmo" %in% colnames(transformed_data))
})

test_that("nlp_elmo_embeddings load", {
  model_files <- list.files("~/cache_pretrained/")
  elmo_model_file <- max(Filter(function(s) startsWith(s, "elmo_"), model_files))
  model <- ml_load(sc, paste0("~/cache_pretrained/", elmo_model_file))
  transformed_data <- ml_transform(model, test_data)
  expect_true("elmo" %in% colnames(transformed_data))
})

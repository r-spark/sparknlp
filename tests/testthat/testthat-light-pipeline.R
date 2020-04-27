setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")
  
  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  embeddings <- nlp_word_embeddings_pretrained(sc, input_cols = c("sentence", "token"), output_col = "embeddings")
  
  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, embeddings)
  fit_pipeline <- ml_fit(pipeline, text_tbl)
  
  assign("sc", sc, envir = parent.frame())
  assign("fit_pipeline", fit_pipeline, envir = parent.frame())
  assign("text_tbl", text_tbl, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(fit_pipeline, envir = .GlobalEnv)
  rm(text_tbl, envir = .GlobalEnv)
})

test_that("nlp_light_pipeline data frame annotate", {
  light_pipeline <- nlp_light_pipeline(fit_pipeline)
  result <- nlp_annotate(light_pipeline, text_tbl, "text")
  expect_true("embeddings" %in% colnames(result))
})

test_that("nlp_light_pipeline pre-trained", {
  pipeline <- nlp_pretrained_pipeline(sc, "explain_document_ml", lang = "en")
  light_pipeline <- nlp_light_pipeline(pipeline)
  result <- nlp_annotate(light_pipeline, "French author who helped pioneer the science-fiction genre. Verne wrate about space, air, and underwater travel before navigable aircrast and practical submarines were invented, and before any means of space travel had been devised.")
  expect_true("token" %in% names(result))
})


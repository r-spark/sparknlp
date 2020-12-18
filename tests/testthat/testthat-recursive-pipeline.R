setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  #pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, embeddings)
  #fit_pipeline <- ml_fit(pipeline, text_tbl)
  
  assign("sc", sc, envir = parent.frame())
  #assign("fit_pipeline", fit_pipeline, envir = parent.frame())
  assign("text_tbl", text_tbl, envir = parent.frame())
})

teardown({
  spark_disconnect(sc)
  rm(sc, envir = .GlobalEnv)
  #rm(fit_pipeline, envir = .GlobalEnv)
  rm(text_tbl, envir = .GlobalEnv)
})

test_that("nlp_recursive_pipeline spark connection", {
  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  
  recursive_pipeline <- nlp_recursive_pipeline(sc) %>%
    ml_add_stage(assembler) %>%
    ml_add_stage(sentdetect) %>% 
    ml_add_stage(tokenizer)
  
  result <- ml_fit_and_transform(recursive_pipeline, text_tbl)
  expect_true("token" %in% colnames(result))
})

test_that("nlp_recursive_pipeline pipeline stages", {
  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  
  recursive_pipeline <- nlp_recursive_pipeline(assembler, sentdetect, tokenizer) 

  result <- ml_fit_and_transform(recursive_pipeline, text_tbl)
  expect_true("token" %in% colnames(result))
})
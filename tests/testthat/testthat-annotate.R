setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")
  
  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer)
  fit_pipeline <- ml_fit(pipeline, text_tbl)
  
  assign("sc", sc, envir = parent.frame())
  assign("fit_pipeline", fit_pipeline, envir = parent.frame())
  assign("text_tbl", text_tbl, envir = parent.frame())
})

teardown({
  spark_disconnect(sc)
  rm(sc, envir = .GlobalEnv)
  rm(fit_pipeline, envir = .GlobalEnv)
  rm(text_tbl, envir = .GlobalEnv)
})

test_that("nlp_annotate text input non-LightPipeline", {
  result <- nlp_annotate(fit_pipeline, "French author who helped pioneer the science-fiction genre. Verne wrate about space, air, and underwater travel before navigable aircrast and practical submarines were invented, and before any means of space travel had been devised.")
  expect_true("token" %in% names(result))
})

test_that("nlp_annotate data frame non-LightPipeline", {
  test_text_frame <- data.frame(texttt = c("The cats are laying in front of the fireplace.",
                                   "The dogs are staying cool in the kitchen."))
  test_frame <- sdf_copy_to(sc, test_text_frame)
  pipeline <- nlp_pretrained_pipeline(sc, "explain_document_ml", lang = "en")
  result <- nlp_annotate(pipeline, test_frame, "texttt")
  expect_true("token" %in% colnames(result))
})

test_that("nlp annotate text input LightPipeline", {
  light_pipeline <- nlp_light_pipeline(fit_pipeline)
  result <- nlp_annotate(light_pipeline, "French author who helped pioneer the science-fiction genre. Verne wrate about space, air, and underwater travel before navigable aircrast and practical submarines were invented, and before any means of space travel had been devised.")
  expect_true("token" %in% names(result))
})

test_that("nlp annotate text list LightPipeline", {
  light_pipeline <- nlp_light_pipeline(fit_pipeline)
  testDoc_list <- c('French author who helped pioner the science-fiction genre.',
                       'Verne wrate about space, air, and underwater travel before navigable aircrast',
                       'Practical submarines were invented, and before any means of space travel had been devised.')
  result <- nlp_annotate(light_pipeline, testDoc_list)
  expect_true("token" %in% names(result[[1]]))
})

test_that("nlp_annotate text input non-LightPipeline", {
  result <- nlp_annotate(fit_pipeline, "French author who helped pioneer the science-fiction genre. Verne wrate about space, air, and underwater travel before navigable aircrast and practical submarines were invented, and before any means of space travel had been devised.")
  expect_true("token" %in% names(result))
})

test_that("nlp_annotate_full text input non-LightPipeline", {
  result <- nlp_annotate_full(fit_pipeline, "French author who helped pioneer the science-fiction genre. Verne wrate about space, air, and underwater travel before navigable aircrast and practical submarines were invented, and before any means of space travel had been devised.")
  expect_true("token" %in% names(result))
})

test_that("nlp_annotate_full data frame non-LightPipeline", {
  test_text_frame <- data.frame(texttt = c("The cats are laying in front of the fireplace.",
                                           "The dogs are staying cool in the kitchen."))
  test_frame <- sdf_copy_to(sc, test_text_frame, "test_text_frame2")
  pipeline <- nlp_pretrained_pipeline(sc, "explain_document_ml", lang = "en")
  result <- nlp_annotate_full(pipeline, test_frame, "texttt")
  expect_true("token" %in% colnames(result))
})

test_that("nlp annotate_full text input LightPipeline", {
  light_pipeline <- nlp_light_pipeline(fit_pipeline)
  result <- nlp_annotate_full(light_pipeline, "French author who helped pioneer the science-fiction genre. Verne wrate about space, air, and underwater travel before navigable aircrast and practical submarines were invented, and before any means of space travel had been devised.")
  expect_true("token" %in% names(result))
})

test_that("nlp annotate text list LightPipeline", {
  light_pipeline <- nlp_light_pipeline(fit_pipeline)
  testDoc_list <- c('French author who helped pioner the science-fiction genre.',
                    'Verne wrate about space, air, and underwater travel before navigable aircrast',
                    'Practical submarines were invented, and before any means of space travel had been devised.')
  result <- nlp_annotate_full(light_pipeline, testDoc_list)
  expect_true("token" %in% names(result[[1]]))
})


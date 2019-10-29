setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("document"), output_col = "token")
  word_embeddings <- nlp_word_embeddings_pretrained(sc, input_cols = c("document", "token"), output_col = "embeddings")
  ner <- nlp_ner_dl_pretrained(sc, input_cols = c("document", "token", "embeddings"), output_col = "ner")
  ner_converter <- nlp_ner_converter(sc, input_cols = c("document", "token", "ner"), output_col = "ner_con")
  
  pipeline <- ml_pipeline(assembler, tokenizer, word_embeddings, ner, ner_converter)
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

test_that("deep_sentence_detector param setting", {
  test_args <- list(
    input_cols = c("string1", "string2", "string3"),
    output_col = "string1",
    include_pragmatic_segmenter = TRUE,
    end_punctuation = c(".", "?"),
    custom_bounds = c("-"),
    explode_sentences = FALSE,
    max_length = 15,
    use_abbreviations = FALSE,
    use_custom_only = FALSE
  )

  test_param_setting(sc, nlp_deep_sentence_detector, test_args)
})

test_that("nlp_deep_sentence_detector spark_connection", {
  test_annotator <- nlp_deep_sentence_detector(sc, input_cols = c("document", "token","ner_con"), output_col = "sentence")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("sentence" %in% colnames(transformed_data))
})

test_that("nlp_deep_sentence_detector ml_pipeline", {
  test_annotator <- nlp_deep_sentence_detector(pipeline, input_cols = c("document", "token","ner_con"), output_col = "sentence")
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("sentence" %in% colnames(transformed_data))
})

test_that("nlp_deep_sentence_detector tbl_spark", {
  transformed_data <- nlp_deep_sentence_detector(test_data, input_cols = c("document", "token","ner_con"), output_col = "sentence")
  expect_true("sentence" %in% colnames(transformed_data))
})


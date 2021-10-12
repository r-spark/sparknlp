setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  embeddings <- nlp_word_embeddings_pretrained(sc, input_cols = c("sentence", "token"), output_col = "embeddings")
  ner_tagger <- nlp_ner_dl_pretrained(sc, input_cols = c("sentence", "token", "embeddings"), output_col = "ner_tags")
  pos_tagger <- nlp_perceptron_pretrained(sc, input_cols = c("sentence", "token"), output_col = "pos")
  dependency_parser <- nlp_dependency_parser_pretrained(sc, input_cols = c("sentence", "pos", "token"), output_col = "dependency")
  typed_dependency_parser <- nlp_typed_dependency_parser_pretrained(sc, input_cols = c("dependency", "pos", "token"), output_col = "dependency_type")
  graph_extractor <- nlp_graph_extraction(sc, input_cols = c("document", "token", "ner_tags"), output_col = "graph")
  
  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, embeddings, ner_tagger, pos_tagger, dependency_parser, 
                          typed_dependency_parser, graph_extractor)
  
  test_data <- ml_fit_and_transform(pipeline, text_tbl)

  assign("sc", sc, envir = parent.frame())
  assign("pipeline", pipeline, envir = parent.frame())
  assign("test_data", test_data, envir = parent.frame())
  assign("text_tbl", text_tbl, envir = parent.frame())
})

teardown({
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
  rm(text_tbl, envir = .GlobalEnv)
})

test_that("graph_finisher param setting", {
  test_args <- list(
    input_col = "string1",
    output_col = "string1",
    clean_annotations = FALSE,
    include_metadata = TRUE,
    output_as_array = TRUE
  )

  test_param_setting(sc, nlp_graph_finisher, test_args)
})

test_that("nlp_graph_finisher spark_connection", {
  test_annotator <- nlp_graph_finisher(sc, input_col = "graph", output_col = "graph_finished")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("graph_finished" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_graph_finisher"))
})

test_that("nlp_graph_finisher ml_pipeline", {
  test_annotator <- nlp_graph_finisher(pipeline, input_col = "graph", output_col = "graph_finished")
  transformed_data <- ml_fit_and_transform(test_annotator, text_tbl)
  expect_true("graph_finished" %in% colnames(transformed_data))
})

test_that("nlp_graph_finisher tbl_spark", {
  transformed_data <- nlp_graph_finisher(test_data, input_col = "graph", output_col = "graph_finished")
  expect_true("graph_finished" %in% colnames(transformed_data))
})


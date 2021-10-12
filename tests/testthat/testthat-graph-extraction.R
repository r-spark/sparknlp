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

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, embeddings, ner_tagger, pos_tagger, dependency_parser, typed_dependency_parser)
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

test_that("graph_extraction param setting", {
  test_args <- list(
    input_cols = c("string1", "string2", "string3"),
    output_col = "string1",
    delimiter = "string1",
    dependency_parser_model = c("string1"),
    entity_types = c("string1", "string2"),
    explode_entities = FALSE,
    include_edges = TRUE,
    max_sentence_size = 200,
    merge_entities = TRUE,
    merge_entities_iob_format = "string1",
    min_sentence_size = 50,
    pos_model = c("string1"),
    relationship_types = c("string1", "string2"),
    root_tokens = c("string1", "string2"),
    typed_dependency_parser_model = c("string1")
  )

  test_param_setting(sc, nlp_graph_extraction, test_args)
})

test_that("nlp_graph_extraction spark_connection", {
  test_annotator <- nlp_graph_extraction(sc, input_cols = c("document", "token", "ner_tags"), output_col = "graph")
  transformed_data <- ml_transform(test_annotator, test_data)
  expect_true("graph" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_graph_extraction"))
})

test_that("nlp_graph_extraction ml_pipeline", {
  test_annotator <- nlp_graph_extraction(pipeline, input_cols = c("document", "token", "ner_tags"), output_col = "graph")
  
  transformed_data <- ml_fit_and_transform(test_annotator, text_tbl)
  expect_true("graph" %in% colnames(transformed_data))
})

test_that("nlp_graph_extraction tbl_spark", {
  transformed_data <- nlp_graph_extraction(test_data, input_cols = c("document", "token", "ner_tags"), output_col = "graph")
  expect_true("graph" %in% colnames(transformed_data))
})


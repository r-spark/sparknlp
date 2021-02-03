setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
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
  rm(sc, envir = .GlobalEnv)
  rm(pipeline, envir = .GlobalEnv)
  rm(test_data, envir = .GlobalEnv)
})

test_that("contextual_parser param setting", {
# TODO: edit these to make them legal values for the parameters
  test_args <- list(
    input_cols = c("string1", "string2"),
    output_col = "string1",
    json_path = "string1",
    #dictionary = "string1",
    case_sensitive = TRUE,
    prefix_and_suffix_match = FALSE,
    context_match = TRUE,
    update_tokenizer = FALSE
  )

  test_param_setting(sc, nlp_contextual_parser, test_args)
})

test_that("contextual_parser setDictionary", {
  annotator <- nlp_contextual_parser(sc, input_cols = c("sentence", "token"), output_col = "entity_gender",
                                     dictionary = "data/gender.csv", read_as = "TEXT", options = list(delimiter = ","))
  expect_true(!is.null(annotator))
})

test_that("nlp_contextual_parser spark_connection", {
  test_annotator <- nlp_contextual_parser(sc, input_cols = c("sentence", "token"), output_col = "entity_gender",
                                          json_path = here::here("tests", "testthat", "data", "gender.json"),
                                          case_sensitive = FALSE, context_match = TRUE,
                                          dictionary = here::here("tests", "testthat", "data", "gender.csv"),
                                          read_as = "TEXT", options = list(delimiter = ","))
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("entity_gender" %in% colnames(transformed_data))
  expect_true(inherits(test_annotator, "nlp_contextual_parser"))
  expect_true(inherits(fit_model, "nlp_contextual_parser_model"))
})

test_that("nlp_contextual_parser ml_pipeline", {
  test_annotator <- nlp_contextual_parser(pipeline, input_cols = c("sentence", "token"), output_col = "entity_gender",
                                          json_path = here::here("tests", "testthat", "data", "gender.json"),
                                          case_sensitive = FALSE, context_match = TRUE,
                                          dictionary = here::here("tests", "testthat", "data", "gender.csv"),
                                          read_as = "TEXT", options = list(delimiter = ","))
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("entity_gender" %in% colnames(transformed_data))
})

test_that("nlp_contextual_parser tbl_spark", {
  transformed_data <- nlp_contextual_parser(test_data, input_cols = c("sentence", "token"), output_col = "entity_gender",
                                          json_path = here::here("tests", "testthat", "data", "gender.json"),
                                          case_sensitive = FALSE, context_match = TRUE,
                                          dictionary = here::here("tests", "testthat", "data", "gender.csv"),
                                          read_as = "TEXT", options = list(delimiter = ","))
  expect_true("entity_gender" %in% colnames(transformed_data))
})


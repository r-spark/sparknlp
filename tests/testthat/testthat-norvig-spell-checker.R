setup({
  sc <- testthat_spark_connection()
  text_tbl <- testthat_tbl("test_text")

  # These lines should set a pipeline that will ultimately create the columns needed for testing the annotator
  assembler <- nlp_document_assembler(sc, input_col = "text", output_col = "document")
  sentdetect <- nlp_sentence_detector(sc, input_cols = c("document"), output_col = "sentence")
  tokenizer <- nlp_tokenizer(sc, input_cols = c("sentence"), output_col = "token")
  normalizer <- nlp_normalizer(sc, input_cols = c("token"), output_col = "normal")

  pipeline <- ml_pipeline(assembler, sentdetect, tokenizer, normalizer)
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

test_that("norvig_spell_checker param setting", {
  test_args <- list(
    input_cols = c("string1"),
    output_col = "string1",
    case_sensitive = FALSE,
    double_variants = FALSE,
    short_circuit = FALSE,
    word_size_ignore = 2,
    dups_limit = 2,
    reduct_limit = 5,
    intersections = 3,
    vowel_swap_limit = 2
  )

  test_param_setting(sc, nlp_norvig_spell_checker, test_args)
})

test_that("nlp_norvig_spell_checker spark_connection", {
  dictionary <- here::here("tests", "testthat", "data", "words.txt")
  test_annotator <- nlp_norvig_spell_checker(sc, input_cols = c("normal"), output_col = "spell", dictionary_path = dictionary)
  fit_model <- ml_fit(test_annotator, test_data)
  transformed_data <- ml_transform(fit_model, test_data)
  expect_true("spell" %in% colnames(transformed_data))
  
  expect_true(inherits(test_annotator, "nlp_norvig_spell_checker"))
  expect_true(inherits(fit_model, "nlp_norvig_spell_checker_model"))
})

test_that("nlp_norvig_spell_checker ml_pipeline", {
  dictionary <- here::here("tests", "testthat", "data", "words.txt")
  test_annotator <- nlp_norvig_spell_checker(pipeline, input_cols = c("normal"), output_col = "spell", dictionary_path = dictionary)
  transformed_data <- ml_fit_and_transform(test_annotator, test_data)
  expect_true("spell" %in% colnames(transformed_data))
})

test_that("nlp_norvig_spell_checker tbl_spark", {
  dictionary <- here::here("tests", "testthat", "data", "words.txt")
  transformed_data <- nlp_norvig_spell_checker(test_data, input_cols = c("normal"), output_col = "spell", dictionary_path = dictionary)
  expect_true("spell" %in% colnames(transformed_data))
})

test_that("nlp_norvig_spell_checker pretrained", {
  model <- nlp_norvig_spell_checker_pretrained(sc, input_cols = c("normal"), output_col = "spell")
  transformed_data <- ml_transform(model, test_data)
  expect_true("spell" %in% colnames(transformed_data))
  
  expect_true(inherits(model, "nlp_norvig_spell_checker_model"))
})


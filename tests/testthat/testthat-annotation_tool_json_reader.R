setup({
  sc <- testthat_spark_connection()
  assign("sc", sc, envir = parent.frame())
  
  if (file.exists(here::here("tests", "testthat", "data", "result.conll"))) {
    file.remove(here::here("tests", "testthat", "data", "result.conll"))
  }
})

teardown({
  rm(sc, envir = .GlobalEnv)
})

test_that("nlp_generate_assertion_read_dataset", {
  train_data_file <- here::here("tests", "testthat", "data", "result.json")
  reader <- nlp_annotation_tool_json_reader(sc)
  mydf <- nlp_annotation_read_dataset(reader, train_data_file)
  
  expect_true("ner_label" %in% colnames(mydf))
})

test_that("nlp_generate_assertion_train_set", {
 train_data_file <- here::here("tests", "testthat", "data", "result.json")
 reader <- nlp_annotation_tool_json_reader(sc)
 mydf <- nlp_annotation_read_dataset(reader, train_data_file)
 train_df <- nlp_generate_assertion_train_set(reader, mydf)

 expect_true("target" %in% colnames(train_df))
})

test_that("nlp_generate_plain_assertion_train_set", {
 train_data_file <- here::here("tests", "testthat", "data", "result.json")
 reader <- nlp_annotation_tool_json_reader(sc)
 mydf <- nlp_annotation_read_dataset(reader, train_data_file)
 train_df <- nlp_generate_plain_assertion_train_set(reader, mydf)

 expect_true("assertion" %in% colnames(train_df))
})

test_that("nlp_generate_colln", {
 train_data_file <- here::here("tests", "testthat", "data", "result.json")
 reader <- nlp_annotation_tool_json_reader(sc)
 mydf <- nlp_annotation_read_dataset(reader, train_data_file)
 train_df <- nlp_generate_colln(reader, mydf, here::here("tests", "testthat", "data", "result.conll"))

 expect_true(file.exists(here::here("tests", "testthat", "data", "result.conll")))
})


test_that("nlp_pubtator_read_dataset", {
  sc <- testthat_spark_connection()
  pubtator <- nlp_pubtator_read_dataset(sc, here::here("tests", "testthat", "data", "corpus_pubtator_sample.txt"))
  expect_true("doc_id" %in% colnames(pubtator))
})
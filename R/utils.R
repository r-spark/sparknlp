#' Set the input column names
#' 
#' @param jobj the object setting the input columns on
#' @param input_cols the input column names
#' 
#' @return the jobj object with the input columns set
#' 
#' @export
nlp_set_input_cols <- function(jobj, input_cols) {
  invoke(spark_jobj(jobj), "setInputCols", cast_string_list(input_cols))
}

#' Set the output column name
#' 
#' @param jobj the object setting the input columns on
#' @param output_col the input column name
#' 
#' @return the jobj object with the output column set
#' 
#' @export
nlp_set_output_col <- function(jobj, output_col) {
  invoke(spark_jobj(jobj), "setOutputCol", cast_string(output_col))
}

#' Spark NLP version
#' 
#' @return the version of the Spark NLP library in use
#' @export
nlp_version <- function() {
  return(spark_nlp_version)
}



# Get a pretrained model.
# The model_class is the Scala class for the model.
pretrained_model <- function(sc, model_class, name = NULL, lang = NULL, remote_loc = NULL) {
  default_name <- invoke(invoke_static(sc, model_class, "defaultModelName"), "x")
  default_lang <- invoke_static(sc, model_class, "defaultLang")
  default_remote_loc <- invoke_static(sc, model_class, "defaultLoc")
  
  if (is.null(name)) name = default_name
  if (is.null(lang)) lang = default_lang
  if (is.null(remote_loc)) remote_loc = default_remote_loc
  
  invoke_static(sc, model_class, "pretrained", name, lang, remote_loc)
}

#' Transform CoNLL format text file to Spark dataframe
#' 
#' In order to train a Named Entity Recognition DL annotator, we need to get CoNLL format data as a spark dataframe. 
#' There is a component that does this for us: it reads a plain text file and transforms it to a spark dataset.
#' See \url{https://nlp.johnsnowlabs.com/docs/en/annotators#conll-dataset}. All the function arguments have defaults. 
#' See \url{https://nlp.johnsnowlabs.com/api/index.html#com.johnsnowlabs.nlp.training.CoNLL} for the defaults.
#' 
#' @param sc a Spark connection
#' @param path path to the file to read
#' @param read_as Can be LINE_BY_LINE or SPARK_DATASET, with options if latter is used (default LINE_BY_LINE)
#' @param document_col name to use for the document column
#' @param sentence_col name to use for the sentence column
#' @param token_col name to use for the token column
#' @param pos_col name to use for the part of speech column
#' @param conll_label_index index position in the file of the ner label
#' @param conll_pos_index index position in the file of the part of speech label
#' @param conll_text_col name to use for the text column
#' @param label_col name to use for the label column
#' @param explode_sentences boolean whether the sentences should be exploded or not
#'  
#' @return Spark dataframe containing the imported data
#' 
#' @export
nlp_conll_read_dataset <- function(sc, path, read_as = NULL, document_col = NULL, sentence_col = NULL, token_col = NULL,
                                   pos_col = NULL, conll_label_index = NULL, conll_pos_index = NULL, conll_text_col = NULL,
                                   label_col = NULL, explode_sentences = NULL) {
  model_class <- "com.johnsnowlabs.nlp.training.CoNLL"
  module <- invoke_static(sc, paste0(model_class, "$"), "MODULE$")
  default_document_col <- invoke(module, "apply$default$1")
  default_sentence_col <- invoke(module, "apply$default$2")
  default_token_col <- invoke(module, "apply$default$3")
  default_pos_col <- invoke(module, "apply$default$4")
  default_conll_label_index <- invoke(module, "apply$default$5")
  default_conll_pos_index <- invoke(module, "apply$default$6")
  default_conll_text_col <- invoke(module, "apply$default$7")
  default_label_col <- invoke(module, "apply$default$8")
  default_explode_sentences <- invoke(module, "apply$default$9")

  document_col <- ifelse(is.null(document_col), default_document_col, document_col)
  sentence_col <- ifelse(is.null(sentence_col), default_sentence_col, sentence_col)
  token_col <- ifelse(is.null(token_col), default_token_col, token_col)
  pos_col <- ifelse(is.null(pos_col), default_pos_col, pos_col)
  conll_label_index <- ifelse(is.null(conll_label_index), default_conll_label_index, conll_label_index)
  conll_pos_index <- ifelse(is.null(conll_pos_index), default_conll_pos_index, conll_pos_index)
  conll_text_col <- ifelse(is.null(conll_text_col), default_conll_text_col, conll_text_col)
  label_col <- ifelse(is.null(label_col), default_label_col, label_col)
  explode_sentences <- ifelse(is.null(explode_sentences), default_explode_sentences, explode_sentences)
  
  conll <- invoke_new(sc, model_class, document_col, sentence_col, token_col,
                      pos_col, conll_label_index, conll_pos_index, conll_text_col,
                      label_col, explode_sentences)
  
  default_read_as <- invoke(conll, "readDataset$default$3")
  read_as <- ifelse(is.null(read_as), default_read_as, read_as)
  
  sdf_register(invoke(conll, "readDataset", spark_session(sc), path, read_as))
}


# sparknlp

<!-- badges: start -->
<!-- badges: end -->

R [sparklyr](https://sparklyr.ai/) extension for using [John Snow Labs](https://www.johnsnowlabs.com/) 
[Spark NLP](https://www.johnsnowlabs.com/spark-nlp) library.

# Installation
Install from this Github repository using:

```
remotes::install_github("r-spark/sparknlp")
```

# Unimplemented features
The following features/annotators have not been implemented yet

## Spark NLP
* WordSegmenter
* DocumentNormalizer
* Spark NLP Display

## Spark NLP for Healthcare
* DocumentLogRegClassifier
* DeIdentificator
* ContextualParser
* RelationExtraction

# Usage
There are a lot of examples in R notebooks inside the `examples` directory. I recommended starting with the 
notebooks in [tutorials/certification_trainings](https://github.com/r-spark/sparknlp/tree/master/examples/tutorials/certification_trainings).

The `examples` directory structure here follows the notebook examples found at
[spark-nlp-workshop](https://github.com/JohnSnowLabs/spark-nlp-workshop).
Note that not all the Jupyter notebooks found there have been ported yet, but all functionality still exists in the 
package.

# Licensed models and annotators
If you have purchased a license to the licensed models and annotators, first follow the 
normal steps in [Spark NLP for Healthcare Getting Started](https://nlp.johnsnowlabs.com/docs/en/licensed_install#install-spark-nlp-for-healthcare).

Once you've done this you should have your AWS-CLI credentials setup and can use the 
licensed pretrained models in the unlicensed annotators (such as ner_dl).

In order to use the licensed annotators you must also setup the environment variable
SPARK_NLP_SECRET_CODE with the secret code provided by John Snow Labs with your
license and SPARK_NLP_LICENSE with the license key.

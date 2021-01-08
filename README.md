
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

# Usage
There are a lot of examples in R notebooks inside the `examples` directory. I recommended starting with the 
notebooks in [tutorials/certification_trainings](https://github.com/r-spark/sparknlp/tree/master/examples/tutorials/certification_trainings).

The `examples` directory structure here follows the notebook examples found at
[spark-nlp-workshop](https://github.com/JohnSnowLabs/spark-nlp-workshop).
Note that not all the Jupyter notebooks found there have been ported yet, but all functionality still exists in the 
package.

# Licensed models and annotators
In order to use the licensed models and annotators you must have a valid license
from John Snow Labs and have followed the steps in [Spark NLP for Healthcare Getting Started](https://nlp.johnsnowlabs.com/docs/en/licensed_install#install-spark-nlp-for-healthcare).

First you must have setup your AWS-CLI credentials in order to use the 
licensed pretrained models.

In order to use the licensed annotators you must also setup the environment variable
SPARK_NLP_SECRET_CODE with the secret code provided by John Snow Labs with your
license and SPARK_NLP_LICENSE with the license key.

In addition, since there is currently a bug in the `sparklyr` package (https://github.com/sparklyr/sparklyr/issues/2882)
you will have to specify the URL to the `spark-nlp-jsl` jar file in the spark config
when you create the spark connection. Here is an example of how you can do it and use the environment
variable instead of hard coding the secret code:

```
config <- sparklyr::spark_config()
config$`sparklyr.shell.driver-memory` <- "16G"

secretCode <- Sys.getenv("SPARK_NLP_SECRET_CODE", unset = NA)
    
if (!is.na(secretCode)) {
  jsl_version <- strsplit(secretCode, "-")[[1]][1]
  jsl_url <- paste0("https://pypi.johnsnowlabs.com/", secretCode, "/spark-nlp-jsl-", jsl_version, ".jar")
  config$`spark.jars` <- c(jsl_url)
}

sc <- sparklyr::spark_connect(master = "local", config = config)
```

once this bug is fixed and released in sparklyr this will be done for you automatically
by the package.

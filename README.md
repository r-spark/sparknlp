
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

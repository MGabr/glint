# [![Glint](https://github.com/rjagerman/glint/wiki/images/glint-logo-small.png "Glint")](https://github.com/MGabr/glint)
[![Build Status](https://travis-ci.com/MGabr/glint.svg?branch=0.2-word2vec)](https://travis-ci.com/MGabr/glint)

Glint is a high performance [Scala](http://www.scala-lang.org/) parameter server built using [Akka](http://akka.io/).
The aim is to make it easy to develop performant distributed machine learning algorithms using the parameter server architecture. One of the major goals is compatibility with [Spark](http://spark.apache.org/).

## Added functionality

This fork adds the following functionality

* Updated dependencies, especially Akka.
* Further Spark integration. Automatically starting Glint servers on Spark workers
or running Glint servers as separate Spark applications.
* Saving of parameter server data to and loading from HDFS.
* Partitioning of matrices by columns.
* [BigWord2VecMatrix](https://mgabr.github.io/glint/latest/api/#glint.models.client.BigWord2VecMatrix)
with operations for network-efficient distributed Word2Vec training for large vocabularies.
Used in the Spark ML package [Glint-word2vec](https://github.com/MGabr/glint-word2vec).
* [BigFMPairMatrix](https://mgabr.github.io/glint/latest/api/#glint.models.client.BigFMPairMatrix)
with operations for network-efficient distributed pairwise factorization machine training for large models.

## Compile
To use the current version you should compile the system manually, publish it to a local repository and include it in your project through sbt. Clone this repository and run:

    sbt compile assembly publishLocal

The command will compile, assemble and publish the library jar file to the local ivy2 repository, which means you can then use it in your project's `build.sbt` (on the same machine) as follows:

    libraryDependencies += "com.github.mgabr" %% "glint" % "0.2-SNAPSHOT"

To also execute integration tests run:

    sbt compile it:test publishLocal

## Documentation

Refer to the original [documentation](http://rjagerma.github.io/glint/) and the [Scaladoc](https://mgabr.github.io/glint/latest/api/) 
for instructions and examples on how to use the software.

## Citation

If you find this project useful in your research, please cite the following paper in your publication(s):

Rolf Jagerman, Carsten Eickhoff and Maarten de Rijke. **"Computing Web-scale Topic Models using an Asynchronous Parameter Server."** *(2017)*

    @inproceedings{jagerman2017computing,
      title={Computing Web-scale Topic Models using an Asynchronous Parameter Server},
      author={Jagerman, Rolf and Eickhoff, Carsten and de Rijke, Maarten},
      booktitle={Proceedings of the 40th International ACM SIGIR Conference on Research and Development in Information Retrieval},
      year={2017},
      organization={ACM}
    }

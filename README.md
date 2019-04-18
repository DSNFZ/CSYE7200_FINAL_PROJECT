# CSYE7200_FINAL_PROJECT

![GitHub commit merge status](https://img.shields.io/github/commit-status/DSNFZ/CSYE7200_FINAL_PROJECT/master/2ba36047ef75fd31e1b17bbaeeb3fe090a1d552b.svg)
![GitHub top language](https://img.shields.io/github/languages/top/DSNFZ/CSYE7200_FINAL_PROJECT.svg)
![GitHub](https://img.shields.io/github/license/DSNFZ/CSYE7200_FINAL_PROJECT.svg)

# Movie Recommendation System

The Movie Recommendation System is the final project of CSYE7200 course. The major services of this system is to make movie recommendation for users according to the rating records by each user, and Search specific movie according to the different searching type and user inputing content. 

## Authors

* **Yumeng Cheng**  - [myc339](https://github.com/myc339)
* **Xiao Hu**  - [DSNFZ](https://github.com/DSNFZ)

See also the list of [contributors](https://github.com/DSNFZ/CSYE7200_FINAL_PROJECT/graphs/contributors) who participated in this project.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 

### Prerequisites

You need to have `scala` and `spark` installed.
* [Scala Installation Instruction](https://www.scala-lang.org/download/)
* [Spark Installation Instruction](http://spark.apache.org/docs/latest/building-spark.html)

You also need to have 'git' installed for version control.
* [Git Installation Instruction](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)

Then the data needed for this project is here, and you need to download them too.
* [Kaggle Movie Data](https://www.kaggle.com/rounakbanik/the-movies-dataset/)

### Installing

First of all, clone the repository to local

```shell
git clone https://github.com/DSNFZ/CSYE7200_FINAL_PROJECT
```

And then, you need to change the `FileConfig.scala` code which defines the path of the data source

```scala
package com.edu.neu.csye7200.finalproject.configure

/**
* Created by IntelliJ IDEA.
* User: dsnfz
* Date: 2019-04-16
* Time: 18:16
* Description: This object store the file path. This can make is easier
*    to modify the file path just by changing the path in this object
*/

object FileConfig {
val dataDir = "input/"
val ratingFile = dataDir + "ratings.csv"
val movieFile = dataDir + "movies_metadata.csv"
val linkFile = dataDir + "links.csv"
val keywordsFile = dataDir + "keywords.csv"
val creditFIle = dataDir + "credits.csv"
}
```
Change the value of `dataDir` to the your data directory.

Finally, you can run the program with `sbt run` in terminal. It will show as following:
```shell
[info] Running com.edu.neu.csye7200.finalproject.Main 
/****************************************/
/****Welcome to MovieRecommendation******/
/****************************************/
/*****Please Log In By Enter UserId******/
/***********Input q to Exit**********/
```

## Running the tests

Three tests class-`DataSpec`, `QuerySpec` and `ALSSpec` contain unit tests for `DataUtil`, `QueryUtil` and `ALSUtil`. `DataSpec` and `QuerySpec` test the basic function of corresponding Util object. And the `ALSSpec` tests the whole process and acceptance criteria of model.

Run the tests as following:

```shell
sbt test
```

## Built With

* [IntelliJ IDEA](https://www.jetbrains.com/idea/) - The IDE to development the system.
* [Spark](https://databricks.com/spark/about) - The framework to develop the Machine Learning process

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details


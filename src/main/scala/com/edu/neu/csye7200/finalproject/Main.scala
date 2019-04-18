package com.edu.neu.csye7200.finalproject
import com.edu.neu.csye7200.finalproject.Interface.MovieRecommendation

import scala.util.control.Breaks._
import scala.util.{Failure, Success, Try}
/**
  * Created by IntelliJ IDEA.
  * User: dsnfz
  * Date: 2019-04-02
  * Time: 15:13
  */

object Main extends App {
  def ToInt(line: String): Try[Int] = {
    Try(line.toInt)
  }

  override def main(args: Array[String]): Unit = {
    //        for(e <- List(1,2)) yield getRecommendation(e)
    //        queryByGenres("Animation").take(5).foreach(x=>println(x._1,x._2))
    //
    //        queryByKeywords("boy").sortWith(_._6>_._6).take(5).foreach(line=>println("id: "+line._1,"popularity: "+
    //          line._6,"title: "+line._3,"keywords: "+line._2,"tagline: "+line._4,"release_date: "+line._5))
    //        for(x<-List("boy") )yield queryByKeywords("boy").take(5).sortBy(_._5.getTime()).foreach(line=>println("id: "+line._1,
    //          "popularity: "+ line._6,"title: "+line._3,"keywords: "+line._2,"tagline: "+line._4,"release_date: "+line._5))
    //    SortBySelected(queryBySpokenLanguages("English").take(5),order="desc").foreach(x=>println("id"+x._1,"spoken_languages"+x._2,x._6))
    //    SortBySelected(queryBySpokenLanguages("English").take(5),order="asc").foreach(x=>println("id"+x._1,"spoken_languages"+x._2,x._6))
    //    println("crew")
    //    queryBystaffInCredits("Andy Tennant","crew").foreach(row=>println(row._1,row._2,row._3))
    //    println("cast")
    //    queryBystaffInCredits("Tom Hanks","cast").foreach(row=>println(row._1,row._2,row._3))
    breakable {
      while (true) {
        println("/****************************************/" +
          "\n/****Welcome to MovieRecommendation******/" +
          "\n/****************************************/" +
          "\n/*****Please Log In By Enter UserId******/" +
          "\n/***********Input q to Exit**********/"
        )
        val id = scala.io.StdIn.readLine()
        if (id.equals("q")) break
        breakable {
          while (true) {
            ToInt(id) match {
              case Success(t) => {

                println("/****************************************/" +
                  "\n/****Welcome to MovieRecommendation******/" +
                  "\n/****************************************/" +
                  "\n/*************User ID " + id + "**********/" +
                  "\n/***********Input q to Exit**********/")
                println("1.Movie Recommendation" +
                  "\n2.Movie Search" +
                  "\n3.Movie Rating(give better recommendation for you)")
                var  num =scala.io.StdIn.readLine()
                if(num.equals("q")) break
                breakable {
                  while (true) {
                    ToInt(num) match {
                      case Success(t)=>{
                        t match{
                          case 1=>{ println("/****Movie Recommendation Result******/" +
                            "\n/****************************************/" +
                            "\n/*************User ID " + id + "**********/" +
                            "\n/***********Input q to Exit**********/")
                            MovieRecommendation.getRecommendation(id.toInt)
                            //                           scala.io.StdIn.readLine()
                             break
                          }
                          case 2=>{
                            breakable {
                              while (true) {
                                println("/****Movie Search Interface ******/" +
                                  "\n/****************************************/" +
                                  "\n/*************User ID " + id + "**********/" +
                                  "\n/***********Input q to Exit**********/")
                                println("1.Search by Keywords" +
                                  "\n2.Search by genres" +
                                  "\n3.Search by production_companies" +
                                  "\n4.Search by production_coutries" +
                                  "\n5.Search by spoken_languages" +
                                  "\n6.Search by title" +
                                  "\n7.Search by cast" +
                                  "\n8.Search by crew"
                                )
                                num = scala.io.StdIn.readLine()
                                if(num.equals("q")) break
                                ToInt(num) match {

                                  case Success(v) => {
                                    println("Enter Content")
                                    val content=scala.io.StdIn.readLine()
                                    v match{
                                      case 1 => MovieRecommendation.queryByKeywords(content).sortWith(_._6 > _._6).take(5).foreach(line => println("id: " + line._1, "popularity: " +
                                        line._6, "title: " + line._3, "keywords: " + line._2, "tagline: " + line._4, "release_date: " + line._5))
                                      case 2 => MovieRecommendation.queryBySelectedInMoviesJson(content, "genres").sortWith(_._6 > _._6).take(5).foreach(line => println("id: " + line._1, "popularity: " +
                                        line._6, "title: " + line._3, "genres: " + line._2, "tagline: " + line._4, "release_date: " + line._5))
                                      case 3 => MovieRecommendation.queryBySelectedInMoviesJson(content, "production_companies").sortWith(_._6 > _._6).take(5).foreach(line => println("id: " + line._1, "popularity: " +
                                        line._6, "title: " + line._3, "production_companies: " + line._2, "tagline: " + line._4, "release_date: " + line._5))
                                      case 4 => MovieRecommendation.queryBySelectedInMoviesJson(content, "production_countries").sortWith(_._6 > _._6).take(5).foreach(line => println("id: " + line._1, "popularity: " +
                                        line._6, "title: " + line._3, "production_coutries: " + line._2, "tagline: " + line._4, "release_date: " + line._5))
                                      case 5 => MovieRecommendation.queryBySelectedInMoviesJson(content, "spoken_languages").sortWith(_._6 > _._6).take(5).foreach(line => println("id: " + line._1, "popularity: " +
                                        line._6, "title: " + line._3, "spoken_languages: " + line._2, "tagline: " + line._4, "release_date: " + line._5))
                                      case 6 => MovieRecommendation.queryBySeletedInMoviesNormal(content, "title").sortWith(_._6 > _._6).take(5).foreach(line => println("id: " + line._1, "popularity: " +
                                        line._6, "title: " + line._2, "tagline: " + line._4, "release_date: " + line._5))
                                      case 7 => MovieRecommendation.queryBystaffInCredits(content, "cast").sortWith(_._6 > _._6).take(5).foreach(line => println("id: " + line._1, "popularity: " +
                                        line._6, "title: " + line._3, "cast: " + line._2, "tagline: " + line._4, "release_date: " + line._5))
                                      case 8 => MovieRecommendation.queryBystaffInCredits(content, "crew").sortWith(_._6 > _._6).take(5).foreach(line => println("id: " + line._1, "popularity: " +
                                        line._6, "title: " + line._3, "cast: " + line._2, "tagline: " + line._4, "release_date: " + line._5))
                                      case _=>break
                                    }

                                  }
                                  case Failure(e)=> break
                                }
                              }
                            }
                          }
                          case 3=>{
                             println("/****Movie Rating Interface******/" +
                              "\n/****************************************/" +
                              "\n/*************User ID " + id + "**********/" +
                              "\n/*************Enter the MovieName you wanna rate**********/"+
                              "\n/***********Input q to Exit**********/")
                              val content=scala.io.StdIn.readLine()
                              if(content.equals("q")) break
                            breakable {
                              while (true) {
                                println("Enter rating you wanna give(0~5)")
                                val rating=scala.io.StdIn.readLine()
                                if(rating.equals("q")) break
                                Try(rating.toFloat) match{
                                  case Success(r)=> {
                                    if(r>=0&&r<=5) {
                                      MovieRecommendation.UpdateRatingsByRecommendation(List(id.toInt.toString, r.toString,
                                        (System.currentTimeMillis()%10000000000.00).toLong.toString), content)
                                      break
                                    }
                                    else {
                                      println("out of range")
                                    }
                                  }
                                  case Failure(r)=>
                                }

                              }
                            }
                          }
                          case _=>break
                        }

                      }
                      case Failure(e)=>break
                    }
                  }
                }
              }
              case Failure(e)=> break
            }

          }
        }
      }
    }
  }
}

package com.hp.s3

import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Result
import com.amazonaws.services.s3.model.S3ObjectSummary
import com.amazonaws.AmazonServiceException
import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.model.S3ObjectInputStream

import collection.JavaConverters._
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.ObjectListing
import java.io.{File, FileNotFoundException, FileOutputStream, IOException}
import java.text.SimpleDateFormat
import java.util.Calendar

import com.hp.utils.Widgets

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.util.Try

object S3Utils {


  val awsS3Credentials = ()=> AmazonS3ClientBuilder.standard.withRegion(Regions.DEFAULT_REGION).build

  def listSubFolders(s3Folder: String) ={
    val s3: AmazonS3 = awsS3Credentials()
    val bucketName = s3Folder.split("/")(0)
    val listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName)
      .withPrefix(s3Folder.replace(bucketName+"/","")).withDelimiter("/")

    val objects = s3.listObjects(listObjectsRequest).getCommonPrefixes

    val returnVal = objects.toArray().map(_.toString)

    returnVal


  }

  def listS3Object(s3Folder: String, filter: Option[String]= None) ={
    val s3: AmazonS3 = awsS3Credentials()
    val bucketName = s3Folder.split("/")(0)
    val listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName)
      .withPrefix(s3Folder.replace(bucketName+"/","")).withDelimiter("/")

    val objects = s3.listObjects(listObjectsRequest).getObjectSummaries()
    val returnVal = filter match{
      case Some(f) => objects.map(x=>x.getKey).toArray.filter(x => x.contains(f))
      case _ => objects.map(x=>x.getKey).toArray
    }


    returnVal


  }

  def deleteS3Object(objectKey:String, filter: Option[String]= None)={

    println("s3 object folder" +objectKey)

    val objectKeys = listS3Object(objectKey.replace("s3://","")
      .replaceAll("$","/").replaceAll("//$","/"), filter)
    println("listing objects to be deleted")
    objectKeys.foreach(println)
    val s3: AmazonS3 = awsS3Credentials()
    import java.util._
    import com.amazonaws.services.s3.model.DeleteObjectsRequest
    val dor: DeleteObjectsRequest = new DeleteObjectsRequest(objectKey.replace("s3://","").split("/")(0))
      .withKeys(objectKeys:_*)


    s3.deleteObjects(dor).getDeletedObjects.forEach(println)
    //println("hello worls"+result.getDeletedObjects().isEmpty)
  }

  def getS3Object(bucket_name:String, key_name:String): Unit ={
    val s3: AmazonS3 = awsS3Credentials()
    try {
      val o = s3.getObject(bucket_name, key_name)
      val s3is = o.getObjectContent()
      val fos = new FileOutputStream(new File(key_name.split("/")(key_name.split("/").length-1)))
      val read_buf = new Array[Byte](1024)
      var read_len =  s3is.read(read_buf)
      while ( read_len > 0) {
        fos.write(read_buf, 0, read_len)
        read_len =  s3is.read(read_buf)
      }
      s3is.close()
      fos.close()
    } catch {
      case e: AmazonServiceException =>
        System.err.println(e.getErrorMessage)
        System.exit(1)
      case e: FileNotFoundException =>
        System.err.println(e.getMessage)
        System.exit(1)
      case e: IOException =>
        System.err.println(e.getMessage)
        System.exit(1)
    }
  }

  def putS3Object(bucket_name:String, key_name:String, filePath:String)={
    val s3: AmazonS3 = awsS3Credentials()
    s3.putObject(bucket_name, key_name, new File(filePath))
  }


}




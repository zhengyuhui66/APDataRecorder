package com.hik.base

import java.util.concurrent.Executors
import com.hik.base.main.APRunable
import com.hik.base.util.{CETCProtocol, ConfigUtil}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * * 说明：
  * * 此项目为AP热点采集记录
  * * 数据分别存储于Redis和Hbase中
  * * 在Hbase中存放Mac采集记录表名分别为
  * * APRecoder:原始记录，以采集设备ID为key
  * * APRecoder_dateInx：以时间为key
  * * APRecoder_macInx：以Mac为key
  * * MTAPInfo：采集到的Mac列表
  * * 在Redis中存放 (注意过期时间)
  * * key：Total_APRecoder  value:mac采集总记录数
  * * key：DayTotal_APRecoderYYYY(年份) value:每年采集总记录数
  * * key：DayTotal_APRecoderYYYYMM(月份) value:每月采集总记录数
  * * key：DayTotal_APRecoderYYYYMMDD(日) value:每日采集总记录数
  * * key：devmin_2019_ap_14306073X00037F23F8E3 value:单设备每年采集总记录数
  * * key：devmin_201902_ap_14306073X00037F23F8E3 value:单设备每月采集总记录数
  * * key：devmin_20190219_ap_14306073X00037F23F8E3 value:单设备每日采集总记录数
  * * key：devmin_2019021913_ap_14306073X00037F23F8E3 value:单设备每时采集总记录数
  * * key：devmin_201902191324_ap_14306073X00037F23F8E3 value:单设备每分采集总记录数
  **/
object App {

  val APRECORDER_CONF:String="dataAnalysis/aprecoder.xml"
  val HBASE_SITE_CONF:String="hbase/hbase-site.xml"
  val APP_NAME="APDataRecorder"
  def main(args:Array[String]):Unit={
    //val path="E:\\BaseDataRecoder\\conf\\"
    val path=args(0)
    val conf = new SparkConf().setAppName(APP_NAME)
    //conf.setMaster("local")
    val sc=new SparkContext(conf)
    val executors = Executors.newCachedThreadPool()
    val apssc=new StreamingContext(sc,Seconds(3))
    val g2016APconf = new ConfigUtil( path+APRECORDER_CONF)
    g2016APconf.setConfPath(path+HBASE_SITE_CONF);
    val G2016ApTopic:APRunable= new APRunable(apssc,g2016APconf,sc)
    executors.submit(G2016ApTopic)

  }
}

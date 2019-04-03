package com.hik.base.bean

import java.text.SimpleDateFormat
import java.util.Date

import com.hik.base.util.{CETCProtocol, CommFunUtils}

class APRecorder {
//  devid: String, mac: String, datetime: Date, ssid: String
  /**
    * 终端mac
    */
  private var mac = ""
  def getMac:String=mac
  /**
    * 终端品牌
    */
  private var bd = ""

  /**
    * 进入时间（采集时间）
    */
  private var ct = ""
  def getCt:String=ct
  /**
    * 离开时间（消失时间）
    */
  private var lt = ""
  def getLt:String=lt
  /**
    * 驻留时长，单位秒
    */
  private var rt = 0L

  /**
    * 终端场强
    */
  private var rssi = ""

  /**
    * 当前连接ssid
    */
  private var ssid = ""
  def getSsid:String=ssid
  /**
    * 热点通道
    */
  private var apc = ""

  /**
    * 热点加密类型
    */
  private var ape = ""

  /**
    * x坐标
    */
  private var x = ""

  /**
    * y坐标
    */
  private var y = ""

  /**
    * 场所编号
    */
  private var nw = ""

  /**
    * 设备编号
    */
  private var devID = ""
  def getDevId:String=devID
  /**
    * 经度
    */
  private var lg = ""
  def getlg:String=lg
  /**
    * 维度
    */
  private var la = ""
  def getla:String=la

  /**
    * 进出位标识
    */
  private var isenter=""
  def getIsenter:String=isenter


  def ToApMacRecoder(array: Array[String]): Unit = {
    mac = CETCProtocol.Escape(array(0)) //38-29-5A-0D-9B-29
    ssid = CETCProtocol.Escape(array(1)) //CMCC-NuxA
    apc = CETCProtocol.Escape(array(2)) //5
    ape = CETCProtocol.Escape(array(3)) //2
    ct = CETCProtocol.GetStrDate(CETCProtocol.IntParse(CETCProtocol.Escape(array(4)))) //1548384668
    rssi = CETCProtocol.Escape(array(5)) //-63
    x = CETCProtocol.Escape(array(6)) //0
    y = CETCProtocol.Escape(array(7)) //0
    nw = CETCProtocol.Escape(array(8)) //42082139000003
    devID = CETCProtocol.Escape(array(9)) //14306073X00037F8364FB
    lg = CETCProtocol.Escape(array(10)) //113.12968
    la = CETCProtocol.Escape(array(11)) //31.02796
    isenter = CETCProtocol.Escape(array(12)) //1
    bd = CETCProtocol.Escape(array(14)) //GUANGDONG OPPO MOBILE TELECOMMUNICATIONS CORP.,LTD
    //isenter为0则为离开，那么当前的进入时间为采集时间，当前的采集时间作为离开时间
    if (isenter != null && isenter.equals(CommFunUtils.EXIT)){
      lt = ct
      ct = CETCProtocol.GetStrDate(CETCProtocol.IntParse(CETCProtocol.Escape(array(13))))
    }else{
      //如果为1则说明还在内部，则采集时间作为进入时间，采集时间加1秒作为离开时间
      lt=CETCProtocol.GetStrDate(CETCProtocol.IntParse(CETCProtocol.Escape(array(4)))+1)
    }
    //离开时间-进入时间作为存留时间，如果在内部（isenter==1）则停留时间设置为1秒
    val teformat:SimpleDateFormat=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    rt=(teformat.parse(lt).getTime-teformat.parse(ct).getTime)/1000
  }
}

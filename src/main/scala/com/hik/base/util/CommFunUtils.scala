package com.hik.base.util

import java.text.SimpleDateFormat
import java.util.{Date, GregorianCalendar}
import com.hik.base.bean.APRecorder
import com.hiklife.utils.{ByteUtil, RedisUtil}
import net.sf.json.JSONObject
import org.apache.hadoop.hbase.client.{HTable, Put}

object CommFunUtils  extends Serializable{

  val ENTER:String="1"
  val EXIT:String="0"
  val AP="ap"
  val MAC="mac"
  val ID="id"
  val MINNAME="devmin"
  val SPLIT="_"

  def byte2HexStr(b:Byte):String={
    var hs=""
    var stmp=(b&0xFF).toHexString.toUpperCase
    hs=if(stmp.length==1){
      hs+"0"+stmp
    }else{
      hs+stmp
    }
    hs
  }

  def putGroupDevId(redisUtil: RedisUtil, m: APRecorder,second:Int,tyTime:String){
    val redisKey = CommFunUtils.MINNAME+CommFunUtils.SPLIT+tyTime+CommFunUtils.SPLIT+CommFunUtils.AP+CommFunUtils.SPLIT+ m.getDevId
    putGroupDevIds(redisUtil,second,redisKey)
  }

  def putGroupDevIds(redisUtil: RedisUtil,second:Int,redisKey:String) = {
    redisUtil.jedis.incr(redisKey)
    redisUtil.jedis.expire(redisKey, second)
  }

  //devid: String, mac: String, datetime: Nothing, `type`: Integer, identification: String
  def putValue(devtable: HTable, rowkey_dev: String, value: String) = {
    val putdev = new Put(rowkey_dev.getBytes)
    putdev.addColumn("RD".getBytes, "IN".getBytes, value.getBytes)
    devtable.put(putdev)
  }


  def putDupValue(devtable: HTable, rowkey_dev: String, value: APRecorder)={
    val putdev = new Put(rowkey_dev.getBytes)
    putdev.addColumn("S".getBytes, "C".getBytes,value.getCt.getBytes)
    putdev.addColumn("S".getBytes, "D".getBytes,value.getDevId.getBytes)
    putdev.addColumn("S".getBytes, "V".getBytes,value.getSsid.getBytes)
    var tJson=new JSONObject();
    tJson.accumulate("lg",value.getlg)
    tJson.accumulate("la",value.getla)
    putdev.addColumn("S".getBytes, "F".getBytes,tJson.toString.getBytes)
    devtable.put(putdev)
  }

  def putDupAPSSIDValue(devtable: HTable, rowkey_dev: String, value: APRecorder)={
    val putdev = new Put(rowkey_dev.getBytes)
    var tJson=new JSONObject();
    tJson.accumulate("ct",value.getCt)
    tJson.accumulate("devID",value.getDevId)
    tJson.accumulate("apmac",value.getMac)
    tJson.accumulate("ssid",value.getSsid)
    putdev.addColumn("S".getBytes, "C".getBytes,tJson.toString.getBytes)
    devtable.put(putdev)
  }


  def putDupDevValue(devtable: HTable, value: APRecorder)={
    val mac=value.getMac.replace("-","")
    val devId=value.getDevId
    val lg=value.getlg
    val la=value.getla
    val vJson=new JSONObject()
    vJson.accumulate("devID",devId)
    vJson.accumulate("la",la)
    vJson.accumulate("lg",lg)
    val putdev = new Put((mac+devId).getBytes)
    putdev.addColumn("S".getBytes, "D".getBytes,vJson.toString.getBytes)

    devtable.put(putdev)
  }


  def getApRecoderByDevRowkey(m:APRecorder) = {

    val devid=m.getDevId.substring(9)
    val mac=m.getMac.replace("-","")
    val datetime=if(m.getIsenter.equals(CommFunUtils.ENTER)){
      m.getCt
    }else{
      m.getLt
    }
    val ssid=m.getSsid
    //var keyrow = byte2HexStr(GetHashCodeWithLimit(devid, 0xFF).asInstanceOf[Byte])
    val bb = new Array[Byte](4)
    ByteUtil.putInt(bb, ("2524608000".toLong - Str2Date(datetime).getTime / 1000).asInstanceOf[Int], 0)
    //keyrow += devid + byte2HexStr(bb) + mac + ssid
    var keyrow=mac+ byte2HexStr(bb) + devid + ssid
    keyrow
  }


  def getApRecoderByDateRowkey(m:APRecorder) = {
    val devid=m.getDevId.substring(9)
    val mac=m.getMac.replace("-","")
    val datetime=if(m.getIsenter.equals(CommFunUtils.ENTER)){
      m.getCt
    }else{
      m.getLt
    }
    val dateTimes=datetime.substring(0,10).replace("-","")
    val ssid=m.getSsid
    var keyrow = byte2HexStr(GetHashCodeWithLimit(dateTimes, 0xFF).asInstanceOf[Byte])
    val bb = new Array[Byte](4)
    ByteUtil.putInt(bb, ("2524608000".toLong - Str2Date(datetime).getTime / 1000).asInstanceOf[Int], 0)
    keyrow += byte2HexStr(bb) + devid + mac + ssid
    keyrow
  }

  def getRowkey(m:APRecorder): String = {
    val devid=m.getDevId.substring(9)
    var keyrow = CommFunUtils.byte2HexStr(CommFunUtils.GetHashCodeWithLimit(devid, 0xFF).asInstanceOf[Byte])
    val bb = new Array[Byte](4)
    val s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    ByteUtil.putInt(bb, ("2524608000".toLong- s.parse(m.getCt).getTime/ 1000).asInstanceOf[Int], 0)
    keyrow += devid + CommFunUtils.byte2HexStr(bb) + m.getMac+m.getSsid
    keyrow
  }
  import com.hiklife.utils.ByteUtil

  /**
    * 根据MAC和时间信息获取对应的rowkey
    *
    * @param devid
    * @param mac
    * @param datetime
    * @return
    */
  def getRowkeyWithMacPrefix(m:APRecorder) = {
    val devid=m.getDevId.substring(9)
    val mac=m.getMac.replace("-","")
    val datetime=if(m.getIsenter.equals(CommFunUtils.ENTER)){
      m.getCt
    }else{
      m.getLt
    }
    val ssid=m.getSsid
    val bb = new Array[Byte](4)
    ByteUtil.putInt(bb, ("2524608000".toLong - Str2Date(datetime).getTime / 1000).asInstanceOf[Int], 0)
    val keyrow = mac + byte2HexStr(bb) + devid + ssid
    keyrow
  }

  def GetHashCodeWithLimit(context: String, limit: Int): Int =  {
    var hash = 0
    for (item <- context.getBytes)  {
      hash = 33 * hash + item
    }
    return (hash % limit)
  }


  def byte2HexStr(b: Array[Byte]): String =  { var hs: String = ""
    var stmp: String = ""
    var n: Int = 0
    for(i<-0 until b.length){
      stmp=(b(i)&0XFF).toHexString
      if(stmp.length==1){
        hs=hs+"0"+stmp
      }else{
        hs=hs+stmp
      }
    }
    return hs.toUpperCase
  }
  /**
    * 字符串(YYYY-MM-DD hh:mm:ss)转换成Date
    *
    * @param s
    * @return
    */
  def Str2Date(s: String): Date ={
    if (!(s == null || (s.equals("")))) try {
      val gc = new GregorianCalendar
      gc.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(s))
      gc.getTime
    } catch {
      case e: Exception =>{
        print(e)
        null
      }
    }
    else null
  }
  //获取当前月时间
  def getYearNowDate():String={
    val s:SimpleDateFormat=new SimpleDateFormat("yyyy")
    s.format(new Date())
  }
  //获取当前月时间
  def getMonthNowDate():String={
    val s:SimpleDateFormat=new SimpleDateFormat("yyyyMM")
    s.format(new Date())
  }
  //获取当前天时间
  def getNowDate():String={
    val s:SimpleDateFormat=new SimpleDateFormat("yyyyMMdd")
    s.format(new Date())
  }
  //根据采集分钟获取
  def getMinNowDate():String={
    val s:SimpleDateFormat=new SimpleDateFormat("yyyyMMddHHmm")
    s.format(new Date())
  }
  //获取小时时间
  def getHourNowDate():String={
    val s:SimpleDateFormat=new SimpleDateFormat("yyyyMMddHH")
    s.format(new Date())
  }

}

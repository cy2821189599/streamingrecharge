package util

import java.util.Properties

object PropertiesUtil {
  /**
   * 获取属性文件的属性值
   * @param propertyName key
   * @param propertyFileName fileName
   * @return value
   */
  def getProperty(propertyName:String,propertyFileName:String) ={
    val properties = new Properties()
    val loader = this.getClass.getClassLoader
    val inputStream = loader.getResourceAsStream(propertyFileName)
    properties.load(inputStream)
    properties.getProperty(propertyName)
  }

}

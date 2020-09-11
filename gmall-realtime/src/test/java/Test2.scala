import com.alibaba.fastjson.JSON

import scala.beans.BeanProperty

/**
 * Author atguigu
 * Date 2020/9/11 15:29
 */
object Test2 {
    def main(args: Array[String]): Unit = {
        val a = A(10, "a")
    
        val json: String = JSON.toJSONString(a, true)
        println(json)
    }
}

case class A(@BeanProperty age: Int,@BeanProperty name: String)

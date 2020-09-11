import com.alibaba.fastjson.JSON

import scala.beans.BeanProperty

/**
 * Author atguigu
 * Date 2020/9/11 15:29
 */
object Test2 {
    def main(args: Array[String]): Unit = {
        /*val a = A(10, "a")
    
        val json: String = JSON.toJSONString(a, true)
        println(json)*/
        val list1 = List(30, 50, 70, 60, 10, 20)
        println(list1.mkString("'","','", "'"))
    }
}

case class A(@BeanProperty age: Int,@BeanProperty name: String)

import org.json4s.JValue
import org.json4s.jackson.{JsonMethods, Serialization}

/**
 * Author atguigu
 * Date 2020/9/7 9:48
 */
object Json4sSemo {
    def main(args: Array[String]): Unit = {
        val json =
            """
              |{
              | "name": "zs",
              | "age": 10
              |}
              |""".stripMargin
    
        /*val j: JValue = JsonMethods.parse(json)
        implicit val f = org.json4s.DefaultFormats
        println((j \ "name").extract[String])*/
       
        implicit val f = org.json4s.DefaultFormats
        val user: User = JsonMethods.parse(json).extract[User]
        println(user)
        
       val s =  Serialization.write(user)
        println(s)
    }
}
case class User(name: String, age: Int)

/*
json4s:
    json for scala
    
    解析
        把json字符串装成sacal的对象
    
    序列化
        把对象转成json字符串
        
        
    


 */

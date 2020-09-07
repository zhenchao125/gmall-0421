/**
 * Author atguigu
 * Date 2020/9/7 15:13
 */
object ReturnDemo1 {
    def main(args: Array[String]): Unit = {
        
        val list1 = List(30, 50, 70, 60, 10, 20)
        try{
            list1.foreach(x => {
                if(x > 60) return
                println(x)
            })
        }catch {
            case e =>
        }
        
        
        println("xxxxxxxxxxxx")
    }
}

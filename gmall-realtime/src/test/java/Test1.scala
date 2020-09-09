/**
 * Author atguigu
 * Date 2020/9/9 11:36
 */
object Test1 {
    def main(args: Array[String]): Unit = {
        val list1 = List(30, 50, 70, 60, 10, 20)
        list1.map(x => x + 1)
        list1.map(_ + 1)
        list1.map(x => +x)
        list1.map(+_)
        
        
        /*list1.foreach(x => x + 1)
        list1.foreach(_ + 1)
        
        list1.foreach(println)
        list1.foreach(println(_))*/
        
        
        // 部分应用函数
        /*val f: Double => Double = math.pow(_, 3)
        println(f(3))
        println(f(4))*/
    
        val f: Any => Unit = println(_)
        f(1)
        
        
        
    }
}

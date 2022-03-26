package com.demo.scala.collection


class ListUsage{

}


/**
 * 列表跟数组类似,但是存在如下区别
 * (1) 列表是不可变的: 列表的元素不能通过赋值改变
 * (2) 列表的结构是递归的
 *
 * 此外跟数组一样,列表还具有如下特性:
 * (1) 同一个列表的所有元素都必须是相同的类型
 * (2) 列表的类型时协变的: 对于一组类型S和T, 如果S是子类型，那么List[S] 就是List[T]的子类型
 */
object ListUsage {

    def main(args: Array[String]): Unit = {

        val fruit = List("apple", "banana", "oranges","abc")
        fruit.filter(name => name.startsWith("a")).foreach(println)

        // 列表构建
        println("=======> constructList ========")
        constructList()
        //初阶方法
        println("========> firstStageApi ========")
        firstStageApi()
        //高阶方法
        println("========> highLevelApi =========")
        ListUsage.highLevelApi()
    }

    /**
     * 列表的构建:
     * 所有的列表构建子两个基础的构建单元： Nil 和 ::(读作"cons")
     */
    def constructList()={
        val fruits: List[String] = "apple" :: ("oranges" :: Nil)
//        println(fruits)

        // 由于操作符::是右结合的,以上写法等效写法如下
        val fruit2: List[String] = "apple" :: "oranges" :: "pear" :: Nil
        // 第一个元素
        println(fruit2.head)
        // 除第一个元素外,后面所有元素
        println(fruit2.tail)
        // 除最后元素外,前面所有元素
        // List为链表方式实现,获取最后元素及前部分需要遍整个列表
        println(fruit2.init)
        // 最后一个元素
        println(fruit2.last)
        println(fruit2.isEmpty)
    }

    /**
     * List初阶方法: 不接收任何参数的方法
     *
     */
    def firstStageApi()={
        // 拼接两个列表
        val list1 = List(1, 2, 3)
        val list2 = List(4, 5, 6)

        val concat: List[Int] = list1 ::: list2
        println(concat.mkString(","))

        // 分治原则示例
        val ints: List[Int] = append(list1, list2)
        println(ints.length)

        // 链表反转
        println(ints.reverse.mkString(","))
        println(ints.mkString("="))

        // 列表前后缀
        val dropInts = ints drop 2
        val takeInts = ints take 3
        // 将列表从下标为的位置拆分为 两个列表
        val splitInts = ints splitAt 3

        println(dropInts.mkString("**"))
        println(takeInts.mkString("--"))
        println(splitInts)

        // 元素选择
        val applyInts = ints apply 0
        val indices: Range = List(3,4,5,6,7,8,9).indices

        println(applyInts)
        println(indices)

        // 将列表扁平化,默认仅做一阶的扁平化
        val list = List(List(1, 2), List(3), List(List(5, 6)))
        val flatten: List[Any] = list.flatten
        println(flatten.mkString(","))

        val fruit: List[String] = "apple" :: ("oranges" :: Nil)
        println(fruit.map(e => {
            e.toCharArray
        }).flatten.mkString(","))

        // 列表拉链操作
        val ints1 = List(1, 2, 3, 4)
        val ints2 = List(4, 3, 2, 1)
        val tuples: List[(Int, Int)] = ints1.zip(ints2)
        println(tuples)
        val unzip: (List[Int], List[Int]) = tuples.unzip
        println(unzip._1)
        println(unzip._2)
        // 将列表和它的小标zip 起来
        val index: List[(String, Int)] = List("a", "b", "c", "d").zipWithIndex
        // toString 和 mkString
        println(index)
        println(ints1.toString)
        println(ints2.mkString("$", ",", ".."))
        println(ints1.mkString)

        // 转换列表 list.toArray, array.toList
        val array: Array[Int] = ints.toArray
        println(array)
        println(array.toList)
        // List 迭代器
        val iterator: Iterator[Int] = ints.iterator
        for(next <- iterator){
            println(next)
        }
    }

    /**
     * List 高阶方法
     *
     */
    def highLevelApi()={
        // 对列表做映射 map, flatMap, foreach
        val addInt: List[Int] = List(1, 2, 3) map (_ + 1)
        println(addInt.mkString(","))

        val strings = List("the", "quick", "brown", "fox")
        println(strings.map(_.length))

        // map返回单个元素的组成的列表
        val list: List[List[Char]] = strings.map(_.toList)
        println(list)
        // flatMap 返回所有元素拼起来的单个列表
        println(list.flatMap(_.toList))

        // list range 方法用来创建 某个区间内所有整数的列表
        println(List.range(3, 6))
        val tuples: List[(Int, Int)] = List.range(1, 5).flatMap(i => List.range(1, i) map (j => (i, j)))
        println(tuples)

        // filter, partition, find, taleWhile , dropWhile 和 span
        println(strings.filter(_.length == 3))
        // partition将List按照是否满足布尔条件拆分为两个List
        val tuple: (List[Int], List[Int]) = List.range(1, 6).partition(_ % 2 == 1)
        println(tuple)

        // find 返回满足条件的第一个元素
        println(List.range(1, 7).find(_ % 2 == 0))
        println(List.range(1, 7).find(_ > 7 ))

        // takeWhile 和 dropWhile 将满足条件的最长前缀take 或者 drop
        println(List(1, 2, 3, -4, 5).takeWhile(_ > 0))
        println(strings.dropWhile(_ startsWith ("t")))

        // forall 和 exists
        println("all num > 0 : " + List(1, 2, 3, -2, 4, 5).forall(_ > 0))
        println("exist num < 0: " + List(1,2,3,-5).exists( _ < 0))

        // 折叠列表 foldLeft , foldRight
        // 使用一种操作符合并元素, 如将列表元素累加
        println(List(1, 2, 3, 4).foldLeft(2)(_ + _))
        println(strings.tail.foldLeft(strings.head)(_ + " " + _))

        // sortWith
        val sort: List[Int] = List(3, -1, 2, 6, 4).sortWith(_ > _)
        println(sort)

        //  tabulate 表格化
        val list1: List[List[Int]] = List.tabulate(5, 5)(_ * _)
        println(list1)

        // concat 等效于 :::
        val ints = List(1, 2, 3, 4)
        println(List.concat(ints, List(-1, -3, -2)))
    }


    /**
     * 分治原则示例
     * 实现与::: 相同操作的append方法
     */
    def append[T](xs:List[T], ys:List[T]):List[T] =
        xs match {
            case List() => ys
            case x::xs1 => x::append(xs1, ys)
        }

}

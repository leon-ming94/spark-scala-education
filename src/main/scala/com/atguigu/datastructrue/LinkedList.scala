package com.atguigu.datastructrue

/**
  * @author shkstart
  */
object LinkedList {
    def main(args: Array[String]): Unit = {
        val node: HeroNode = new HeroNode(1,"heihei","heihei")

    }
}

class SingleLinkedList(){
    val head = new HeroNode(-1,"","")

    def isEmply(): Boolean ={
        head.next == null
    }

    def delete(heroNode: HeroNode): Unit ={
        //判断链表是否为空
        //

    }
}


class HeroNode(var no: Int, var name: String, var nickname: String) {

    var next: HeroNode = null

}
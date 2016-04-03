package com.knoldus

import org.scalatest.FunSuite

class MountainTest extends FunSuite{

  test("Connection with the client"){

    val result = Mountaineer.getClient().toString
    assert("[org.elasticsearch.client.transport.TransportClient@5ac86ba5]"===result)
  }

  test("Insertion into the index"){

    val result = Mountaineer.insert("xyz",8)
    assert(result===true)
  }

  test("Updation into the index"){

    val result = Mountaineer.update("Mountain Kanchenjunga",6)
    assert(result===true)
  }

  test("Searching in the index"){

    val result = Mountaineer.search("2")
    assert(result===true)
  }

  test("Deletion in the index"){

    val result = Mountaineer.delete(6)
    assert(result===true)
  }

  test("Count of all the documents"){

    val result = Mountaineer.getAll()
    assert(result===8)
  }
}

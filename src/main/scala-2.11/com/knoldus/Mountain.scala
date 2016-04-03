package com.knoldus

import java.net.InetAddress

import org.elasticsearch.action.deletebyquery.{DeleteByQueryAction}
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.plugin.deletebyquery.DeleteByQueryPlugin



class Mountain {

  def getClient():Client ={

    val settings:Settings= Settings.settingsBuilder().put("cluster.name","sonu").build()
    val client: Client = TransportClient.builder().settings(settings).addPlugin(classOf[DeleteByQueryPlugin]).build()
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"),9300))
    client
  }

  def insert(name:String, id:Int):Boolean={

    val doc =
      s"""
         |{
         |"name":"$name",
         |"id":$id
         |}
       """.stripMargin
    val response = getClient().prepareIndex("mountaineers","mountain",id.toString).setSource(doc).get()
    val result = Integer.parseInt(response.getId)
    if(result==id) true else false
  }

  def update(newName:String,id:Int):Boolean={

    val doc =
      s"""
         |{
         |"name":"$newName",
         |"id":$id
         |}
       """.stripMargin

    val response = getClient().prepareUpdate("mountaineers","mountain",id.toString).setDoc(doc).get()
    val result = response.getVersion
    if(result>1) true else false
  }

  def search(id:String):Boolean={

    val response = getClient().prepareSearch("mountaineers").setTypes("mountain").setQuery(
      QueryBuilders.termQuery("id",s"$id")
    ).execute().actionGet()
    val result = response.getHits.getTotalHits
    if(result==1)  true else false
  }

  def delete(id:Int):Boolean={

    val query = QueryBuilders.termsQuery("_id",id.toString)
    val response= DeleteByQueryAction.INSTANCE
      .newRequestBuilder(getClient())
      .setIndices("mountaineers")
      .setTypes("mountain")
      .setQuery(query).get()
    val result = response.getTotalDeleted
    if(result == 1) true else false
  }

  def getAll():Long={

    val response=getClient().prepareSearch("mountaineers").execute().actionGet().getHits.totalHits()
    response
  }
}

object Mountaineer extends Mountain


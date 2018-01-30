package com.wavesplatform.http

import com.wavesplatform.http.ApiMarshallers._
import com.wavesplatform.settings.FunctionalitySettings
import com.wavesplatform.state2._
import com.wavesplatform.state2.diffs.newHistory
import com.wavesplatform.state2.reader.SnapshotStateReader
import com.wavesplatform.{BlockGen, TestDB}
import org.scalamock.scalatest.MockFactory
import org.scalatest.prop.PropertyChecks
import play.api.libs.json.JsObject
import scorex.api.http.BlockNotExists
import scorex.consensus.nxt.api.http.NxtConsensusApiRoute

class ConsensusRouteSpec extends RouteSpec("/consensus") with RestAPISettingsHelper with TestDB with PropertyChecks with MockFactory with BlockGen with HistoryTest {
  private val state = mock[SnapshotStateReader]

  private val history = newHistory()
//  appendGenesisBlock(history)
//  for (i <- 1 to 10) appendTestBlock(history)

  private val route = NxtConsensusApiRoute(restAPISettings, state, history, FunctionalitySettings.TESTNET).route

  routePath("/generationsignature") - {
    "for last block" in {
      Get(routePath("/generationsignature")) ~> route ~> check {
        (responseAs[JsObject] \ "generationSignature").as[String] shouldEqual history.lastBlock.get.consensusData.generationSignature.base58
      }
    }

    "for existed block" in {
      val block = history.blockAt(3).get
      Get(routePath(s"/generationsignature/${block.uniqueId.base58}")) ~> route ~> check {
        (responseAs[JsObject] \ "generationSignature").as[String] shouldEqual block.consensusData.generationSignature.base58
      }
    }

    "for not existed block" in {
      Get(routePath(s"/generationsignature/brggwg4wg4g")) ~> route should produce(BlockNotExists)
    }
  }

  routePath("/basetarget") - {
    "for existed block" in {
      val block = history.blockAt(3).get
      Get(routePath(s"/basetarget/${block.uniqueId.base58}")) ~> route ~> check {
        (responseAs[JsObject] \ "baseTarget").as[Long] shouldEqual block.consensusData.baseTarget
      }
    }

    "for not existed block" in {
      Get(routePath(s"/basetarget/brggwg4wg4g")) ~> route should produce(BlockNotExists)
    }
  }
}

package com.wavesplatform.matcher.settings

import com.typesafe.config.ConfigFactory
import com.wavesplatform.settings.{LogLevel, WavesSettings}
import org.scalatest.{FlatSpec, Matchers}

class WavesSettingsSpecification extends FlatSpec with Matchers {
  private val home = System.getenv("HOME")

  "WavesSettings" should "read values from default config" in {
    val config = ConfigFactory.load()
    val settings = WavesSettings.fromConfig(config)

    settings.directory should be(home + "/waves")
    settings.loggingLevel should be(LogLevel.INFO)
    settings.networkSettings should not be null
    settings.walletSettings should not be null
    settings.blockchainSettings should not be null
    settings.checkpointsSettings should not be null
    settings.feesSettings should not be null
    settings.matcherSettings should not be null
    settings.minerSettings should not be null
    settings.restAPISettings should not be null
    settings.synchronizationSettings should not be null
    settings.utxSettings should not be null
  }

}
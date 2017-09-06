package com.wavesplatform.state2.diffs

import java.util

import cats.kernel.Monoid
import com.wavesplatform.state2.reader.SnapshotStateReader
import com.wavesplatform.state2.{ByteStr, Diff, LeaseInfo, StateWriter}
import scorex.account.{Address, Alias}
import scorex.block.Block
import scorex.transaction.{History, PaymentTransaction}

import scala.collection.mutable

class TestState extends SnapshotStateReader with StateWriter with History {
  private val blocks = mutable.Buffer.empty[Block]
  private var diff = Diff.empty

  override def score = ???

  override def scoreOf(blockId: ByteStr) = ???

  override def lastBlockHeaderAndSize = ???

  override def blockHeaderAndSize(height: Int) = ???

  override def blockHeaderAndSize(blockId: ByteStr) = ???

  override def lastBlock = blocks.lastOption

  override def blockBytes(height: Int) =
    if (height <= 0 || blocks.lengthCompare(height) > 0) None else Some(blocks(height - 1).bytes())

  override def blockBytes(blockId: ByteStr) =
    blocks.find(_.uniqueId == blockId).map(_.bytes())

  override def heightOf(blockId: ByteStr) = Option(blocks.indexWhere(_.uniqueId == blockId)).filter(_ > -1)

  override def lastBlockIds(howMany: Int) = ???

  override def blockIdsAfter(parentSignature: ByteStr, howMany: Int) = ???

  override def parent(ofBlock: Block, back: Int) = ???

  override protected val activationWindowSize: Int = 0

  override def approvedFeatures() = Map.empty

  override def featureVotesCountWithinActivationWindow(height: Int) = ???

  override def append(blockDiff: Diff, block: Block): Unit = {
    blocks += block
    diff = Monoid.combine(diff, blockDiff)
  }

  override def rollbackTo(targetBlockId: ByteStr) = ???

  override def status = ???

  override def transactionInfo(id: ByteStr) = ???

  override def containsTransaction(id: ByteStr) = ???

  override def assetDescription(id: ByteStr) = ???

  override def wavesBalance(a: Address) = diff.portfolios.get(a).fold(0L)(_.balance)

  override def assetBalance(a: Address) = diff.portfolios.get(a).fold(Map.empty[ByteStr, Long])(_.assets)

  override def nonZeroLeaseBalances = ???

  override def height = blocks.length

  override def accountTransactionIds(a: Address, limit: Int) =
    diff.accountTransactionIds.getOrElse(a, Seq.empty)

  override def paymentTransactionIdByHash(hash: ByteStr) =
    blocks.view.flatMap(_.transactionData).collectFirst {
      case p: PaymentTransaction if util.Arrays.equals(p.hash(), hash.arr) => p.id()
    }

  override def aliasesOfAddress(a: Address) = ???

  override def resolveAlias(a: Alias) = ???

  override def leaseDetails(leaseId: ByteStr) = ???

  override def leaseInfo(a: Address) = diff.portfolios.get(a).fold(LeaseInfo(0, 0))(_.leaseInfo)

  override def activeLeases = ???

  override def leaseOverflows = Map.empty

  override def leasesOf(address: Address) = ???

  override def lastUpdateHeight(acc: Address) = ???

  override def snapshotAtHeight(acc: Address, h: Int) = ???

  override def filledVolumeAndFee(orderId: ByteStr) = ???
}

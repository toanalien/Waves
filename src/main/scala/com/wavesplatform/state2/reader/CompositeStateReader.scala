package com.wavesplatform.state2.reader

import cats.implicits._
import cats.kernel.Monoid
import com.wavesplatform.state2._
import scorex.account.{Address, Alias}
import scorex.transaction.Transaction
import scorex.transaction.assets.IssueTransaction
import scorex.transaction.lease.LeaseTransaction

class CompositeStateReader private(inner: SnapshotStateReader, diff: Diff) extends SnapshotStateReader {

  override def assetDescription(id: ByteStr) = {
    inner
      .assetDescription(id).orElse(diff.transactions.get(id).collectFirst {
        case (_, it: IssueTransaction, _) => AssetDescription(it.sender, it.name, it.decimals, it.reissuable)
      })
      .map(z => diff.issuedAssets.get(id).fold(z)(r => z.copy(reissuable = r.isReissuable)))

  }

  override def leaseOverflows = {
    val innerAddressesWithOverflows = inner.leaseOverflows.keys
    val addressesWithNewLeases = diff.portfolios.collect {
      case (address, portfolio) if portfolio.leaseInfo.leaseOut > 0 => address
    }

    (innerAddressesWithOverflows ++ addressesWithNewLeases)
      .map(a => a -> (wavesBalance(a) - leaseInfo(a).leaseOut))
      .filter { case (a, overflow) => overflow < 0 }
      .toMap
  }

  override def leasesOf(address: Address) = {
    val innerLeases = inner.leasesOf(address)
    val leaseChanges = diff.leaseState.flatMap {
      case (id, false) => Some(id -> innerLeases(id).copy(isActive = false))
      case (id, true) =>
        diff.transactions.get(id) match {
          case Some((h, lt: LeaseTransaction, _)) => Some(id -> LeaseDetails(lt.sender, lt.recipient, h, lt.amount, true))
          case _ => None
        }
    }

    innerLeases ++ leaseChanges
  }

  override def nonZeroLeaseBalances = inner.nonZeroLeaseBalances ++ diff.portfolios.collect {
    case (addr, p) if p.leaseInfo != LeaseInfo.empty => addr -> p.leaseInfo
  }

  override def leaseDetails(leaseId: ByteStr) = diff.transactions.get(leaseId) match {
    case Some((h, l: LeaseTransaction, _)) => Some(LeaseDetails(l.sender, l.recipient, h, l.amount, diff.leaseState.getOrElse(leaseId, false)))
    case _ => inner.leaseDetails(leaseId)
  }

  override def leaseInfo(a: Address) = Monoid.combine(diff.portfolios.get(a).fold(Monoid.empty[LeaseInfo])(p => p.leaseInfo), inner.leaseInfo(a))

  override def transactionInfo(id: ByteStr): Option[(Int, Option[Transaction])] =
    diff.transactions.get(id)
      .map(t => (t._1, Some(t._2)))
      .orElse(inner.transactionInfo(id))

  override def height: Int = inner.height + 1

  override def accountTransactionIds(a: Address, limit: Int): Seq[ByteStr] = {
    val fromDiff = diff.accountTransactionIds.get(a).orEmpty
    if (fromDiff.lengthCompare(limit) >= 0) {
      fromDiff.take(limit)
    } else {
      fromDiff ++ inner.accountTransactionIds(a, limit - fromDiff.size) // fresh head ++ stale tail
    }
  }

  override def wavesBalance(a: Address) = {
    val innerBalance = inner.wavesBalance(a)
    diff.portfolios.get(a).fold(innerBalance)(_.balance + innerBalance)
  }


  override def assetBalance(a: Address) = {
    Monoid.combine(inner.assetBalance(a), diff.portfolios.get(a).fold(Map.empty[ByteStr, Long])(_.assets))
  }

  override def snapshotAtHeight(acc: Address, h: Int): Option[Snapshot] = ???

  override def paymentTransactionIdByHash(hash: ByteStr): Option[ByteStr]
  = diff.paymentTransactionIdsByHashes.get(hash)
    .orElse(inner.paymentTransactionIdByHash(hash))

  override def aliasesOfAddress(a: Address): Seq[Alias] =
    diff.aliases.filter(_._2 == a).keys.toSeq ++ inner.aliasesOfAddress(a)

  override def resolveAlias(a: Alias): Option[Address] = diff.aliases.get(a).orElse(inner.resolveAlias(a))

  override def activeLeases: Seq[ByteStr] = {
    diff.leaseState.collect { case (id, isActive) if isActive => id }.toSeq ++ inner.activeLeases
  }

  override def lastUpdateHeight(acc: Address): Option[Int] = ???

  override def containsTransaction(id: ByteStr): Boolean = diff.transactions.contains(id) || inner.containsTransaction(id)

  override def filledVolumeAndFee(orderId: ByteStr): OrderFillInfo =
    diff.orderFills.get(orderId).orEmpty.combine(inner.filledVolumeAndFee(orderId))
}

object CompositeStateReader {
  def composite(diff: Diff, inner: SnapshotStateReader): SnapshotStateReader = new CompositeStateReader(inner, diff)
}

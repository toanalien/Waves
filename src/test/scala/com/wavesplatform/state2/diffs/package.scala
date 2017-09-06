package com.wavesplatform.state2

import com.wavesplatform.settings.FunctionalitySettings
import com.wavesplatform.state2.reader.SnapshotStateReader
import scorex.block.Block
import scorex.settings.{TestFunctionalitySettings => TFS}
import scorex.transaction.{History, ValidationError}

package object diffs {
  def newState(): History with SnapshotStateReader with StateWriter = new TestState

  def newHistory(): History = newState()

  val ENOUGH_AMT: Long = Long.MaxValue / 3

  def assertDiffEi(preconditions: Seq[Block], block: Block, fs: FunctionalitySettings = TFS.Enabled)
                  (assertion: Either[ValidationError, Diff] => Unit): Unit = {
    val fp = newHistory()
    val state = newState()
    def differ(s: SnapshotStateReader, b: Block) = BlockDiffer.fromBlock(fs, fp, s, None, b)

    preconditions.foreach { precondition =>
      val preconditionDiffEI = differ(state, precondition)
      val preconditionDiff = preconditionDiffEI.explicitGet()
      state.append(preconditionDiff, precondition)
    }
    val totalDiff1 = differ(state, block)
    assertion(totalDiff1)
  }

  def assertDiffAndState(preconditions: Seq[Block], block: Block, fs: FunctionalitySettings = TFS.Enabled)
                        (assertion: (Diff, SnapshotStateReader) => Unit): Unit = {
    val fp = newHistory()
    val state = newState()

    def differ(s: SnapshotStateReader, b: Block) = BlockDiffer.fromBlock(fs, fp, s, None, b)

    preconditions.foreach { precondition =>
      val preconditionDiff = differ(state, precondition).explicitGet()
      state.append(preconditionDiff, precondition)
    }
    val totalDiff1 = differ(state, block).explicitGet()
    state.append(totalDiff1, block)
    assertion(totalDiff1, state)
  }

  def produce(errorMessage: String): ProduceError = new ProduceError(errorMessage)

  def zipWithPrev[A](seq: Seq[A]): Seq[(Option[A], A)] = {
    seq.zipWithIndex.map { case ((a, i)) => (if (i == 0) None else Some(seq(i - 1)), a) }
  }
}

using QuIXI.MQ;
using IXICore;
using IXICore.Meta;
using IXICore.Activity;

namespace QuIXI.Meta
{
    internal class ICTransactionInclusionCallbacks : TransactionInclusionCallbacks
    {
        public void transactionVerified(Transaction tx)
        {
            var bh = IxianHandler.getBlockHeader(tx.applied);
            Node.activityStorage.updateStatus(tx.id, ActivityStatus.Final, tx.applied, bh.timestamp);

            if (IxianHandler.isMyAddress(tx.pubKey))
            {
                foreach (var fromEntry in tx.fromList)
                {
                    IxianHandler.balances.FirstOrDefault(x => x.address != null && x.address.SequenceEqual(new Address(tx.pubKey.getInputBytes(), fromEntry.Key)))?.lastUpdate = 0;
                }
            }
            else
            {
                foreach (var toEntry in tx.toList)
                {
                    if (IxianHandler.isMyAddress(toEntry.Key))
                    {
                        IxianHandler.balances.FirstOrDefault(x => x.address != null && x.address.SequenceEqual(toEntry.Key))?.lastUpdate = 0;
                    }
                }
            }

            var obj = new Dictionary<string, string>();
            obj.Add(tx.getTxIdString(), "verified");

            Node.messageQueue.PublishAsync(MQTopics.TransactionStatusUpdate, obj);
        }

        public void transactionRejected(Transaction tx)
        {
            tx.applied = 0;
            Node.activityStorage.updateStatus(tx.id, ActivityStatus.Error, 0);

            var obj = new Dictionary<string, string>();
            obj.Add(tx.getTxIdString(), "rejected");

            Node.messageQueue.PublishAsync(MQTopics.TransactionStatusUpdate, obj);
        }

        public void transactionExpired(Transaction tx)
        {
            tx.applied = 0;
            Node.activityStorage.updateStatus(tx.id, ActivityStatus.Error, 0);

            var obj = new Dictionary<string, string>();
            obj.Add(tx.getTxIdString(), "expired");

            Node.messageQueue.PublishAsync(MQTopics.TransactionStatusUpdate, obj);
        }

        public void receivedBlockHeader(Block blockHeader, bool verified)
        {
            foreach (Balance balance in IxianHandler.balances)
            {
                if (balance.blockChecksum != null && balance.blockChecksum.SequenceEqual(blockHeader.blockChecksum))
                {
                    balance.verified = true;
                }
            }

            if (blockHeader.blockNum + 10 >= IxianHandler.getHighestKnownNetworkBlockHeight()
                && (IxianHandler.status == NodeStatus.warmUp || IxianHandler.status == NodeStatus.stalled))
            {
                IxianHandler.status = NodeStatus.ready;
            }

            Node.messageQueue.PublishAsync(MQTopics.BlockHeader, blockHeader);

            // if block pruning is not enabled, we can prune old block signatures and TxIDs to save space.
            if (!Node.tiv.pruneBlocks
                && blockHeader.blockNum % CoreConfig.maxBlockHeadersPerDatabase == 0)
            {
                ulong fullBlocksToKeep = 4000;
                if (blockHeader.blockNum > fullBlocksToKeep)
                {
                    ulong pruneBlocksBelow = blockHeader.blockNum - fullBlocksToKeep;
                    Logging.info("Pruning block signatures up to block " + pruneBlocksBelow + " at height " + blockHeader.blockNum);
                    Node.storage.pruneBlocks(pruneBlocksBelow, IXICore.Storage.BlockSigPruningType.Signatures, false);
                    Logging.info("Pruning TxIDs up to block " + pruneBlocksBelow + " at height " + blockHeader.blockNum);
                    Node.storage.pruneTxIDs(pruneBlocksBelow);
                }

                ulong PoCWBlocksToKeep = 100000;
                if (blockHeader.blockNum > PoCWBlocksToKeep)
                {
                    ulong pruneBlocksBelow = blockHeader.blockNum - PoCWBlocksToKeep;
                    Logging.info("Pruning block PoCW up to block " + pruneBlocksBelow + " at height " + blockHeader.blockNum);
                    Node.storage.pruneBlocks(pruneBlocksBelow, IXICore.Storage.BlockSigPruningType.PoCW, false);
                }
            }
        }

        public void blockReorg(Block blockHeader)
        {
            var revertedTransactions = Node.activityStorage.revertTransactionsByBlockHeight(blockHeader.blockNum);
            Node.messageQueue.PublishAsync(MQTopics.BlockReorg, blockHeader);
            foreach(var revertedTx in revertedTransactions)
            {
                var activity = Node.activityStorage.getActivityById(revertedTx, null, true);
                PendingTransactions.addOutgoingTransaction(activity.transaction, activity.transaction.toList.TakeLast(2).Select(x => x.Key).ToList());

                var obj = new Dictionary<string, string>();
                obj.Add(activity.transaction.getTxIdString(), "reverted");
                Node.messageQueue.PublishAsync(MQTopics.TransactionStatusUpdate, obj);
            }
        }
    }
}

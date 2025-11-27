using QuIXI.MQ;
using IXICore;
using IXICore.Meta;
using IXICore.Storage;
using IXICore.Streaming;

namespace QuIXI.Meta
{
    internal class ICTransactionInclusionCallbacks : TransactionInclusionCallbacks
    {
        public void receivedTIVResponse(Transaction tx, bool verified)
        {
            if (!verified)
            {
                tx.applied = 0;
                //Node.activityStorage.updateStatus(tx.id, ActivityStatus.Error, 0);
                return;
            }
            else
            {
                PendingTransactions.remove(tx.id);
            }

            TransactionCache.addTransaction(tx);
            Friend friend = FriendList.getFriend(tx.pubKey);
            if (friend == null)
            {
                foreach (var toEntry in tx.toList)
                {
                    friend = FriendList.getFriend(toEntry.Key);
                    if (friend != null)
                    {
                        break;
                    }
                }
            }
            var obj = new Dictionary<string, bool>();
            obj.Add(tx.getTxIdString(), verified);

            Node.messageQueue.PublishAsync(MQTopics.TransactionStatusUpdate, obj);

            IxianHandler.balances.First().lastUpdate = 0;

            //var bh = IxianHandler.getBlockHeader(tx.applied);
            //Node.activityStorage.updateStatus(tx.id, status, tx.applied, bh.timestamp);
        }

        public void receivedBlockHeader(Block block_header, bool verified)
        {
            foreach (Balance balance in IxianHandler.balances)
            {
                if (balance.blockChecksum != null && balance.blockChecksum.SequenceEqual(block_header.blockChecksum))
                {
                    balance.verified = true;
                }
            }

            if (block_header.blockNum >= IxianHandler.getHighestKnownNetworkBlockHeight())
            {
                IxianHandler.status = NodeStatus.ready;
            }
            Node.processPendingTransactions();
        }
    }
}

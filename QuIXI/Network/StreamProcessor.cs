using QuIXI.Meta;
using QuIXI.MQ;
using IXICore;
using IXICore.Meta;
using IXICore.Network;
using IXICore.Streaming;
using System.Text;
using IXICore.Streaming.Models;
using System;

namespace QuIXI.Network
{
    class StreamProcessor : CoreStreamProcessor
    {
        public StreamProcessor(PendingMessageProcessor pendingMessageProcessor, StreamCapabilities streamCapabilites) : base(pendingMessageProcessor, streamCapabilites)
        {
        }

        // Called when receiving S2 data from clients
        public override ReceiveDataResponse? receiveData(byte[] bytes, RemoteEndpoint endpoint, bool fireLocalNotification = true, bool alert = true)
        {
            ReceiveDataResponse? rdr = base.receiveData(bytes, endpoint, fireLocalNotification);
            if (rdr == null)
            {
                return rdr;
            }

            StreamMessage message = rdr.streamMessage;
            SpixiMessage spixi_message = rdr.spixiMessage;
            Friend? friend = rdr.friend;
            Address sender_address = rdr.senderAddress;
            Address? group_sender_address = rdr.groupSenderAddress;

            if (friend != null)
            {
                if (endpoint != null)
                {
                    // Update friend's last seen and relay if outgoing stream capabilities are disabled
                    if ((streamCapabilities & StreamCapabilities.Outgoing) == 0)
                    {
                        friend.updatedStreamingNodes = Clock.getNetworkTimestamp();
                        friend.relayNode = new Peer(endpoint.getFullAddress(true), endpoint.serverWalletAddress, Clock.getTimestamp(), Clock.getTimestamp(), Clock.getTimestamp(), 0);
                        friend.online = true;
                    }
                }
            }
            
            try
            {
                switch (spixi_message.type)
                {
                    case SpixiMessageCode.requestFundsResponse:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.RequestFundsResponse, message);
                        }
                        break;

                    case SpixiMessageCode.fileHeader:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.FileHeader, message);
                        }
                        break;

                    case SpixiMessageCode.acceptFile:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.AcceptFile, message);
                            break;
                        }

                    case SpixiMessageCode.requestFileData:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.RequestFileData, message);
                            break;
                        }

                    case SpixiMessageCode.fileData:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.FileData, message);
                            break;
                        }

                    case SpixiMessageCode.fileFullyReceived:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.FileFullyReceived, message);
                            break;
                        }

                    case SpixiMessageCode.appData:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.AppData, message);
                            break;
                        }

                    case SpixiMessageCode.appProtocolData:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.AppProtocolData, message);
                            break;
                        }

                    case SpixiMessageCode.appRequest:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.AppRequest, message);
                            break;
                        }

                    case SpixiMessageCode.appRequestAccept:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.AppRequestAccept, message);
                            break;
                        }

                    case SpixiMessageCode.appRequestReject:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.AppRequestReject, message);
                            break;
                        }
                    case SpixiMessageCode.appEndSession:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.AppEndSession, message);
                            break;
                        }

                    case SpixiMessageCode.appRequestError:
                        Node.messageQueue.PublishAsync(MQTopics.AppRequestError, message);
                        break;

                    case SpixiMessageCode.requestAdd:
                    case SpixiMessageCode.requestAdd2:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.RequestAdd2, message);

                            CoreProtocolMessage.resubscribeEvents();
                            fetchFriendsPresence(friend);
                        }
                        break;

                    case SpixiMessageCode.acceptAdd:
                    case SpixiMessageCode.acceptAdd2:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.AcceptAdd2, message);

                            CoreProtocolMessage.resubscribeEvents();
                            fetchFriendsPresence(friend);
                        }
                        break;

                    case SpixiMessageCode.keys:
                    case SpixiMessageCode.keys2:
                        {
                            CoreProtocolMessage.resubscribeEvents();
                            fetchFriendsPresence(friend);
                        }
                        break;

                    case SpixiMessageCode.acceptAddBot:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.AcceptAddBot, message);
                        }
                        break;

                    case SpixiMessageCode.botAction:
                        Node.messageQueue.PublishAsync(MQTopics.BotAction, message);
                        break;

                    case SpixiMessageCode.msgTyping:
                        Node.messageQueue.PublishAsync(MQTopics.MsgTyping, message);
                        break;

                    case SpixiMessageCode.avatar:
                        if (spixi_message.data != null && spixi_message.data.Length < 500000)
                        {
                            Node.messageQueue.PublishAsync(MQTopics.Avatar, message);
                        }
                        else
                        {
                            //FriendList.setAvatar(sender_address, null, null, real_sender_address);
                        }
                        break;

                    case SpixiMessageCode.requestFunds:
                        Node.addMessageWithType(message.id, FriendMessageType.requestFunds, sender_address, 0, UTF8Encoding.UTF8.GetString(spixi_message.data));
                        Node.messageQueue.PublishAsync(MQTopics.RequestFunds, message);
                        break;

                    case SpixiMessageCode.sentFunds:
                        CoreProtocolMessage.broadcastGetTransaction(spixi_message.data, 0, endpoint);
                        Node.addMessageWithType(message.id, FriendMessageType.sentFunds, sender_address, 0, Transaction.getTxIdString(spixi_message.data));

                        Node.messageQueue.PublishAsync(MQTopics.SentFunds, message);
                        break;

                    case SpixiMessageCode.chat:
                        if (Node.addMessageWithType(message.id, FriendMessageType.standard, sender_address, spixi_message.channel, Encoding.UTF8.GetString(spixi_message.data), false, group_sender_address, message.timestamp, fireLocalNotification) != null)
                        {
                            Node.messageQueue.PublishAsync(MQTopics.Chat, message);
                        }
                        if (friend != null && !friend.bot)
                        {
                            sendReceivedConfirmation(friend, message.id, spixi_message.channel);
                        }
                        break;

                    case SpixiMessageCode.chatStream:
                        {
                            var csm = new ChatStreamMessage(spixi_message.data);
                            var fm = Node.addMessageWithType(FriendMessageType.standard, sender_address, spixi_message.channel, csm, false, group_sender_address, message.timestamp, fireLocalNotification, 0);
                            if (fm != null)
                            {
                                Node.messageQueue.PublishAsync(MQTopics.ChatStream, message);
                            }
                            if (friend != null && !friend.bot)
                            {
                                if (fm == null)
                                {
                                    fm = friend.getMessage(spixi_message.channel, csm.MessageId);
                                    if (fm != null)
                                    {
                                        if (fm.sequence >= csm.Sequence)
                                        {
                                            // already have this message or a newer one, ignore
                                            sendReceivedConfirmation(friend, message.id, spixi_message.channel);
                                            return null;
                                        }
                                        else
                                        {
                                            // message is newer than what we have, with a sequence gap, wait for missing messages
                                            return null;
                                        }
                                    }
                                }
                                sendReceivedConfirmation(friend, message.id, spixi_message.channel);
                            }
                        }
                        break;

                    case SpixiMessageCode.msgReceived:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.MsgReceived, message);
                        }
                        break;

                    case SpixiMessageCode.msgRead:
                        {
                            Node.messageQueue.PublishAsync(MQTopics.MsgRead, message);
                        }
                        break;

                    case SpixiMessageCode.msgDelete:
                        Node.messageQueue.PublishAsync(MQTopics.MsgDelete, message);
                        break;

                    case SpixiMessageCode.msgReaction:
                        Node.messageQueue.PublishAsync(MQTopics.MsgReaction, message);
                        break;

                    case SpixiMessageCode.leaveConfirmed:
                        Node.messageQueue.PublishAsync(MQTopics.LeaveConfirmed, message);
                        break;

                    case SpixiMessageCode.nick:
                        Node.messageQueue.PublishAsync(MQTopics.Nick, message);
                        break;

                    case SpixiMessageCode.getAppProtocols:
                        Node.messageQueue.PublishAsync(MQTopics.GetAppProtocols, message);
                        break;

                    case SpixiMessageCode.appProtocols:
                        Node.messageQueue.PublishAsync(MQTopics.AppProtocols, message);
                        break;

                    case SpixiMessageCode.transactionRequest:
                        Node.messageQueue.PublishAsync(MQTopics.TransactionRequest, message);
                        break;

                    case SpixiMessageCode.transactionSend:
                        Node.messageQueue.PublishAsync(MQTopics.Transaction, new TransactionSend(spixi_message.data).Transaction);
                        break;

                    case SpixiMessageCode.transactionSendRequest:
                        Node.messageQueue.PublishAsync(MQTopics.TransactionSendRequest, message);
                        break;

                    case SpixiMessageCode.transactionSendResponse:
                        Node.messageQueue.PublishAsync(MQTopics.TransactionSendResponse, message);
                        break;

                    default:
                        Logging.warn("Unknown SpixiMessageCode {0}", spixi_message.type);
                        break;
                }
            }
            catch (Exception e)
            {
                Logging.error("Exception occured in StreamProcessor.receiveData: " + e);
            }
            return rdr;
        }
    }
}

using QuIXI.Meta;
using IXICore;
using IXICore.Streaming;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using IXICore.Streaming.Models;
using IXICore.Meta;

namespace QuIXI
{
    class APIServer : GenericAPIServer
    {
        protected override bool processRequest(HttpListenerContext context, string methodName, Dictionary<string, object> parameters)
        {
            JsonResponse response = null;

            if (methodName.Equals("contacts", StringComparison.OrdinalIgnoreCase))
            {
                response = onContacts(parameters);
            }

            if (methodName.Equals("addContact", StringComparison.OrdinalIgnoreCase))
            {
                response = onAddContact(parameters);
            }

            if (methodName.Equals("acceptContact", StringComparison.OrdinalIgnoreCase))
            {
                response = onAcceptContact(parameters);
            }

            if (methodName.Equals("removeContact", StringComparison.OrdinalIgnoreCase))
            {
                response = onRemoveContact(parameters);
            }

            if (methodName.Equals("sendChatMessage", StringComparison.OrdinalIgnoreCase))
            {
                response = onSendChatMessage(parameters);
            }

            if (methodName.Equals("sendChatStreamMessage", StringComparison.OrdinalIgnoreCase))
            {
                response = onSendChatStreamMessage(parameters);
            }

            if (methodName.Equals("sendSpixiMessage", StringComparison.OrdinalIgnoreCase))
            {
                response = onSendSpixiMessage(parameters);
            }

            if (methodName.Equals("sendAppData", StringComparison.OrdinalIgnoreCase))
            {
                response = onSendAppData(parameters);
            }

            if (methodName.Equals("getLastMessages", StringComparison.OrdinalIgnoreCase))
            {
                response = onGetLastMessages(parameters);
            }

            if (response == null)
            {
                return false;
            }

            // Set the content type to plain to prevent xml parsing errors in various browsers
            context.Response.ContentType = "application/json";

            sendResponse(context.Response, response);

            context.Response.Close();

            return true;
        }

        private JsonResponse onContacts(Dictionary<string, object> parameters)
        {
            return new JsonResponse { result = FriendList.friends, error = null };
        }


        private JsonResponse onAddContact(Dictionary<string, object> parameters)
        {
            JsonError? error = null;

            if (!parameters.ContainsKey("address"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "address parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            Address address = new ExtendedAddress((string)parameters["address"]).RoutingAddress;
            var contactName = address.ToString();
            Friend? friend = FriendList.addFriend(FriendType.Normal, FriendState.RequestSent, address, null, contactName, null, null, 0);

            if (friend != null)
            {
                friend.save();

                CoreStreamProcessor.sendContactRequest(friend);

                Node.addMessageWithType(null, FriendMessageType.requestAddSent, address, 0, "", true);

                return new JsonResponse { result = friend, error = null };
            }
            else
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "contact already added" };
                return new JsonResponse { result = null, error = error };
            }
        }

        private JsonResponse onAcceptContact(Dictionary<string, object> parameters)
        {
            JsonError? error = null;

            if (!parameters.ContainsKey("address"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "address parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            Address address = new ExtendedAddress((string)parameters["address"]).RoutingAddress;
            Friend? friend = FriendList.getFriend(address);

            if (friend == null)
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "contact doesn't exist" };
                return new JsonResponse { result = null, error = error };
            }
            friend.approved = true;
            CoreStreamProcessor.sendAcceptAdd2(friend);

            return new JsonResponse { result = friend, error = null };
        }


        private JsonResponse onRemoveContact(Dictionary<string, object> parameters)
        {
            JsonError? error = null;

            if (!parameters.ContainsKey("address"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "address parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            Address address = new ExtendedAddress((string)parameters["address"]).RoutingAddress;
            Friend? friend = FriendList.getFriend(address);
            if (friend == null || !FriendList.removeFriend(friend))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "contact doesn't exist" };
                return new JsonResponse { result = null, error = error };
            }

            return new JsonResponse { result = friend, error = null };
        }


        private JsonResponse onSendChatMessage(Dictionary<string, object> parameters)
        {
            JsonError? error = null;

            if (!parameters.ContainsKey("address"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "address parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            if (!parameters.ContainsKey("message"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "message parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }


            if (!parameters.ContainsKey("channel"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "channel parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            Address address = new ExtendedAddress((string)parameters["address"]).RoutingAddress;
            Friend? friend = FriendList.getFriend(address);

            if (friend == null)
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "contact doesn't exist" };
                return new JsonResponse { result = null, error = error };
            }

            int channel = int.Parse((string)parameters["channel"]);
            string message = (string)parameters["message"];

            FriendMessage? friend_message = Node.addMessageWithType(null, FriendMessageType.standard, friend.walletAddress, channel, message, true, null, 0, true, UTF8Encoding.UTF8.GetBytes(message).Length);

            CoreStreamProcessor.sendChatMessage(friend, friend_message, channel);

            return new JsonResponse { result = friend_message, error = null };
        }

        private JsonResponse onSendChatStreamMessage(Dictionary<string, object> parameters)
        {
            JsonError? error = null;

            if (!parameters.ContainsKey("address"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "address parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            if (!parameters.ContainsKey("message"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "message parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            if (!parameters.ContainsKey("channel"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "channel parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            bool isStream = false;
            if (parameters.ContainsKey("isStream"))
            {
                isStream = bool.Parse((string)parameters["isStream"]);
            }

            Address address = new ExtendedAddress((string)parameters["address"]).RoutingAddress;
            Friend? friend = FriendList.getFriend(address);
            int channel = int.Parse((string)parameters["channel"]);

            byte[]? message_id = null;
            int sequence = 0;
            if (parameters.ContainsKey("messageId"))
            {
                message_id = Convert.FromBase64String((string)parameters["messageId"]);
                if (parameters.ContainsKey("sequence"))
                {
                    sequence = int.Parse((string)parameters["sequence"]);
                }
                else
                {
                    var fm = friend.getMessage(channel, message_id);
                    if (fm == null)
                    {
                        if (sequence > 0)
                        {
                            error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "messageId doesn't exist or sequence is missing" };
                            return new JsonResponse { result = null, error = error };
                        }
                    }
                    else
                    {
                        sequence = fm.sequence + 1;
                    }
                }
            }

            if (friend == null)
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "contact doesn't exist" };
                return new JsonResponse { result = null, error = error };
            }

            string message = (string)parameters["message"];
            var csm = new ChatStreamMessage(message_id, message, sequence, isStream);
            FriendMessage? friend_message = Node.addMessageWithType(FriendMessageType.standard, friend.walletAddress, channel, csm, true, IxianHandler.primaryWalletAddress, 0, true, UTF8Encoding.UTF8.GetBytes(message).Length);
            csm.MessageId = friend_message.id;

            CoreStreamProcessor.sendChatStreamMessage(friend, csm, channel);

            return new JsonResponse { result = friend_message, error = null };
        }

        private JsonResponse onSendSpixiMessage(Dictionary<string, object> parameters)
        {
            JsonError? error = null;

            if (!parameters.ContainsKey("address"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "address parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            if (!parameters.ContainsKey("type"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "type parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            if (!parameters.ContainsKey("data"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "data parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            if (!parameters.ContainsKey("channel"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "channel parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            bool addToPendingMessages = false;
            bool sendToServer = false;
            bool sendPushNotification = false;
            bool removeAfterSending = true;

            if (parameters.ContainsKey("addToPendingMessages"))
            {
                addToPendingMessages = bool.Parse((string)parameters["addToPendingMessages"]);
            }

            if (parameters.ContainsKey("sendToServer"))
            {
                sendToServer = bool.Parse((string)parameters["sendToServer"]);
            }

            if (parameters.ContainsKey("sendPushNotification"))
            {
                sendPushNotification = bool.Parse((string)parameters["sendPushNotification"]);
            }

            if (parameters.ContainsKey("removeAfterSending"))
            {
                removeAfterSending = bool.Parse((string)parameters["removeAfterSending"]);
            }

            Address address = new ExtendedAddress((string)parameters["address"]).RoutingAddress;
            Friend? friend = FriendList.getFriend(address);

            if (friend == null)
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "contact doesn't exist" };
                return new JsonResponse { result = null, error = error };
            }

            SpixiMessageCode type = (SpixiMessageCode)int.Parse((string)parameters["type"]);
            byte[] data = Crypto.stringToHash((string)parameters["data"]);
            int channel = int.Parse((string)parameters["channel"]);

            CoreStreamProcessor.sendSpixiMessage(friend,
                                                 new SpixiMessage(type, data, channel),
                                                 null,
                                                 null,
                                                 addToPendingMessages,
                                                 sendToServer,
                                                 sendPushNotification,
                                                 removeAfterSending);

            return new JsonResponse { result = friend, error = null };
        }


        private JsonResponse onSendAppData(Dictionary<string, object> parameters)
        {
            // TOOD Add multi addresses
            JsonError? error = null;

            if (!parameters.ContainsKey("address"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "address parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            byte[] session_id = null;
            byte[] protocol_id = null;
            if (parameters.ContainsKey("appId"))
            {
                session_id = CryptoManager.lib.sha3_512sqTrunc(UTF8Encoding.UTF8.GetBytes((string)parameters["appId"]));
            }
            else if (parameters.ContainsKey("protocolId"))
            {
                protocol_id = CryptoManager.lib.sha3_512Trunc(UTF8Encoding.UTF8.GetBytes((string)parameters["protocolId"]));
            }
            else
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "appId or protocolId parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            if (!parameters.ContainsKey("data"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "data parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            Address address = new ExtendedAddress((string)parameters["address"]).RoutingAddress;
            Friend? friend = FriendList.getFriend(address);

            if (friend == null)
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "contact doesn't exist" };
                return new JsonResponse { result = null, error = error };
            }

            byte[] data = UTF8Encoding.UTF8.GetBytes((string)parameters["data"]);
            if (session_id != null)
            {
                CoreStreamProcessor.sendAppData(friend, session_id, data);
            }
            else
            {
                CoreStreamProcessor.sendAppProtocolData(friend, protocol_id, data);
            }

            return new JsonResponse { result = friend, error = null };
        }

        private JsonResponse onGetLastMessages(Dictionary<string, object> parameters)
        {
            JsonError? error = null;

            if (!parameters.ContainsKey("address"))
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "address parameter is missing" };
                return new JsonResponse { result = null, error = error };
            }

            int count = 10;
            if (parameters.ContainsKey("count"))
            {
                count = int.Parse((string)parameters["count"]);
            }

            int channel = 0;
            if (parameters.ContainsKey("channel"))
            {
                channel = int.Parse((string)parameters["channel"]);
            }

            Address address = new ExtendedAddress((string)parameters["address"]).RoutingAddress;
            Friend? friend = FriendList.getFriend(address);
            if (friend == null)
            {
                error = new JsonError { code = (int)RPCErrorCode.RPC_INVALID_PARAMS, message = "contact doesn't exist" };
                return new JsonResponse { result = null, error = error };
            }

            return new JsonResponse { result = friend.getMessages(channel, count), error = null };
        }
    }
}

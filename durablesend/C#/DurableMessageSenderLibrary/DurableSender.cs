//---------------------------------------------------------------------------------
// Microsoft (R)  Windows Azure Platform AppFabric SDK
// Software Development Kit
// 
// Copyright (c) Microsoft Corporation. All rights reserved.  
//
// THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, 
// EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED WARRANTIES 
// OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE. 
//---------------------------------------------------------------------------------

namespace Microsoft.ServiceBus.Samples.DurableMessageSender
{
    using System;
    using System.Messaging;
    using System.Threading;
    using System.Transactions;
    using Microsoft.ServiceBus.Messaging;

    public class DurableMessageSender : IDisposable
    {
        const bool enableFaultInjection = true;
        const long minTimerWaitTimeInMilliseconds = 50;
        const long maxTimerWaitTimeInMilliseconds = 60000;

        MessagingFactory messagingFactory;
        MessageSender messageSender;
        MessageQueue msmqQueue;
        MessageQueue msmqDeadletterQueue;
        string sbusEntityName;
        string msmqQueueName;
        string msmqDeadletterQueueName;
        Timer waitAfterErrorTimer;
        long timerWaitTimeInMilliseconds;
        BrokeredMessage clonedMessage;

        // FOR TESTING PURPOSE ONLY.
        FaultInjector faultInjector;

        enum SendResult {Success, WaitAndRetry, PermanentFailure};

        public DurableMessageSender(MessagingFactory messagingFactory, string serviceBusQueueOrTopicName)
        {
            this.messagingFactory = messagingFactory;
            this.sbusEntityName = serviceBusQueueOrTopicName;

            // Create a Service Bus queue client to send messages to the Service Bus queue.
            this.messageSender = this.messagingFactory.CreateMessageSender(this.sbusEntityName);

            // Create MSMQ queue if it doesn't exit. If it does, open the existing MSMQ queue.
            this.msmqQueueName = MsmqHelper.CreateMsmqQueueName(this.sbusEntityName, "SEND");
            this.msmqQueue = MsmqHelper.GetMsmqQueue(this.msmqQueueName);

            // Create MSMQ deadletter queue if it doesn't exit. If it does, open the existing MSMQ deadletter queue.
            this.msmqDeadletterQueueName = MsmqHelper.CreateMsmqQueueName(this.sbusEntityName, "SEND_DEADLETTER");
            this.msmqDeadletterQueue = MsmqHelper.GetMsmqQueue(this.msmqDeadletterQueueName);

            // Initialize wait time after durable client experienced a transient error.
            timerWaitTimeInMilliseconds = minTimerWaitTimeInMilliseconds;

            // FOR TESTING PURPOSE ONLY.
            this.faultInjector = new FaultInjector(enableFaultInjection);

            // Start receiving messages from the MSMQ queue.
            MsmqPeekBegin();
        }

        public void Dispose()
        {
            this.messageSender.Close();
            GC.SuppressFinalize(this);
        }

        public void DeleteMsmqQueues()
        {
            Console.WriteLine("DurableMessageSender: Deleting MSMQ queue {0} ...\n", this.msmqQueueName);
            MessageQueue.Delete(this.msmqQueueName);
            Console.WriteLine("DurableMessageSender: Deleting MSMQ queue {0} ...\n", this.msmqDeadletterQueueName);
            MessageQueue.Delete(this.msmqDeadletterQueueName);
        }

        public void Send(BrokeredMessage sbusMessage)
        {
            Message msmqMessage = MsmqHelper.PackSbusMessageIntoMsmqMessage(sbusMessage);
            SendtoMsmq(this.msmqQueue, msmqMessage);
        }

        void SendtoMsmq(MessageQueue msmqQueue, Message msmqMessage)
        {
            if (Transaction.Current == null)
            {
                msmqQueue.Send(msmqMessage, MessageQueueTransactionType.Single);
            }
            else
            {
                msmqQueue.Send(msmqMessage, MessageQueueTransactionType.Automatic);
            }
        }

        void MsmqPeekBegin()
        {
            this.msmqQueue.BeginPeek(TimeSpan.FromSeconds(60), null, MsmqOnPeekComplete);
        }

        void MsmqOnPeekComplete(IAsyncResult result)
        {
            // Complete the MSMQ peek operation. If a timeout occured, peek again.
            Message msmqMessage = null;
            try
            {
                msmqMessage = this.msmqQueue.EndPeek(result);
            }
            catch (MessageQueueException ex)
            {
                if (ex.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                {
                    MsmqPeekBegin();
                    return;
                }
            }

            if (msmqMessage != null)
            {
                BrokeredMessage sbusMessage = MsmqHelper.UnpackSbusMessageFromMsmqMessage(msmqMessage);
                SendMessageToServiceBus(sbusMessage);
            }
        }

        void SendMessageToServiceBus(BrokeredMessage sbusMessage)
        {
            // Clone Service Bus message in case we need to deadletter it or send it again.
            this.clonedMessage = CloneBrokeredMessage(sbusMessage);

            Console.WriteLine("DurableMessageSender: Enqueue message {0} into Service Bus.", sbusMessage.Label);
            try
            {
                // FOR TESTING PURPOSE ONLY: Inject Service Bus error. 
                faultInjector.InjectFaultBeforeSendingMessageToServiceBus();

                messageSender.BeginSend(sbusMessage, ServiceBusOnSendComplete, sbusMessage.Label);
            }
            catch
            {
                // Permanent failure. Move message into deadletter queue. Then remove the message from the MSMQ queue.
                Console.WriteLine("DurableMessageSender: Permanent error when sending message to Service Bus. Deadletter message."); 
                DeadletterMessage(this.clonedMessage);
                this.msmqQueue.BeginReceive(TimeSpan.FromSeconds(60), null, MsmqOnReceiveComplete);
            }
        }

        void ServiceBusOnSendComplete(IAsyncResult result)
        {
            string label = (string)result.AsyncState;

            switch (ServiceBusEndSendMessage(result))
            {
                case SendResult.Success: // Message was successfully sent to Service Bus. Remove MSMQ message from MSMQ queue.
                    Console.WriteLine("DurableMessageSender: Service Bus send operation completed for message {0}.", label);
                    timerWaitTimeInMilliseconds = minTimerWaitTimeInMilliseconds; // Reset timout.
                    this.msmqQueue.BeginReceive(TimeSpan.FromSeconds(60), null, MsmqOnReceiveComplete);
                    break;
                case SendResult.WaitAndRetry: // Service Bus is temporarily unavailable. Wait.
                    Console.WriteLine("DurableMessageSender: Service Bus is temporarily unavailable when sending message {0}. Wait {1}ms.", label, timerWaitTimeInMilliseconds);
                    waitAfterErrorTimer = new Timer(ResumeSendingMessagesToServiceBus, null, timerWaitTimeInMilliseconds, Timeout.Infinite);
                    timerWaitTimeInMilliseconds = Math.Min(2 * timerWaitTimeInMilliseconds, maxTimerWaitTimeInMilliseconds); // Double timeout.
                    break;
                case SendResult.PermanentFailure: // Permanent error. Deadletter MSMQ message, then remove MSMQ message from MSMQ queue.
                    Console.WriteLine("DurableMessageSender: Permanent error when sending message {0} to Service Bus. Deadletter message.", label);
                    DeadletterMessage(this.clonedMessage);
                    this.msmqQueue.BeginReceive(TimeSpan.FromSeconds(60), null, MsmqOnReceiveComplete);
                    break;
            }
        }

        // Removal of MSMQ message from MSMQ queue completed. Peek next MSMQ message.
        void MsmqOnReceiveComplete(IAsyncResult result)
        {
            this.msmqQueue.EndReceive(result);
            Console.WriteLine("DurableMessageSender: MSMQ receive operation completed.");
            MsmqPeekBegin();
        }
        
        // Enqueue message in deadletter queue.
        void DeadletterMessage(BrokeredMessage sbusMessage)
        {
            Message msmqDeadletterMessage = MsmqHelper.PackSbusMessageIntoMsmqMessage(sbusMessage);
            try
            {
                SendtoMsmq(this.msmqDeadletterQueue, msmqDeadletterMessage);
            }
            catch (Exception ex)
            {
                Console.WriteLine("DurableMessageSender: Failure when sending message {0} to deadletter queue {1}: {2} {3}",
                    msmqDeadletterMessage.Label, msmqDeadletterQueue.FormatName, ex.GetType(), ex.Message);
            }
        }

        SendResult ServiceBusEndSendMessage(IAsyncResult result)
        {
            string label = (string)result.AsyncState;
            try
            {
                this.messageSender.EndSend(result);

                // FOR TESTING PURPOSE ONLY: Inject Service Bus error. 
                faultInjector.InjectFaultAfterSendingMessageToServiceBus();

                return SendResult.Success;
            }
            catch (MessagingException ex)
            {
                if (ex.IsTransient)
                {
                    Console.WriteLine("DurableMessageSender: Transient exception when sending message {0}: {1} {2}", label, ex.GetType(), ex.Message);
                    return SendResult.WaitAndRetry;
                }
                else
                {
                    Console.WriteLine("DurableMessageSender: Permanent exception when sending message {0}: {1} {2}", label, ex.GetType(), ex.Message);
                    return SendResult.PermanentFailure;
                }
            }

            catch (Exception ex)
            {
                Type exceptionType = ex.GetType();
                if (exceptionType == typeof(TimeoutException))
                {
                    Console.WriteLine("DurableMessageSender: Exception: {0}", exceptionType);
                    return SendResult.WaitAndRetry;
                }
                else
                {
                    // Indicate a permanent failure in case of:
                    //  - ArgumentException
                    //  - ArgumentNullException
                    //  - ArgumentOutOfRangeException
                    //  - InvalidOperationException
                    //  - OperationCanceledException
                    //  - TransactionException
                    //  - TransactionInDoubtException
                    //  - TransactionSizeExceededException
                    //  - UnauthorizedAccessException
                    Console.WriteLine("DurableMessageSender: Exception: {0}", exceptionType);
                    return SendResult.PermanentFailure;
                }
            }
        }

        // Timer expired. Send cloned message to Service Bus.
        void ResumeSendingMessagesToServiceBus(Object stateInfo)
        {
            Console.WriteLine("DurableMessageSender: Resume peeking MSMQ messages.");
            SendMessageToServiceBus(this.clonedMessage);
        }

        // Clone BrokeredMessage including system properties.
        BrokeredMessage CloneBrokeredMessage(BrokeredMessage source)
        {
            BrokeredMessage destination = source.Clone();
            destination.ContentType = source.ContentType;
            destination.CorrelationId = source.CorrelationId;
            destination.Label = source.Label;
            destination.MessageId = source.MessageId;
            destination.ReplyTo = source.ReplyTo;
            destination.ReplyToSessionId = source.ReplyToSessionId;
            destination.ScheduledEnqueueTimeUtc = source.ScheduledEnqueueTimeUtc;
            destination.SessionId = source.SessionId;
            destination.TimeToLive = source.TimeToLive;
            destination.To = source.To;

            return destination;
        }
    }
}

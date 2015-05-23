package org.flite.mock.amazonaws.sqs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.AmazonWebServiceRequest;
import com.amazonaws.ResponseMetadata;
import com.amazonaws.regions.Region;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.model.AddPermissionRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityBatchResult;
import com.amazonaws.services.sqs.model.ChangeMessageVisibilityRequest;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequest;
import com.amazonaws.services.sqs.model.DeleteMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.DeleteMessageBatchResult;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesRequest;
import com.amazonaws.services.sqs.model.GetQueueAttributesResult;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.GetQueueUrlResult;
import com.amazonaws.services.sqs.model.InvalidAttributeNameException;
import com.amazonaws.services.sqs.model.ListDeadLetterSourceQueuesRequest;
import com.amazonaws.services.sqs.model.ListDeadLetterSourceQueuesResult;
import com.amazonaws.services.sqs.model.ListQueuesRequest;
import com.amazonaws.services.sqs.model.ListQueuesResult;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.amazonaws.services.sqs.model.ReceiptHandleIsInvalidException;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.RemovePermissionRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.amazonaws.services.sqs.model.SendMessageBatchResult;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.amazonaws.util.Md5Utils;

/**
 * Copyright (c) 2012 Flite, Inc
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 *
 * Currently, there is <em>NO</em> plan to implement any time-based features in this mock:
 *  - scheduling
 *  - delays
 *  - retrieved messages returning to the queue
 */
public class AmazonSQSMock implements AmazonSQS {

    public static final String NYI_EXCEPTION = "Not Yet Implemented!\n" +
            "If you'd like to contribute your code, please send a pull request to our repo:\n" +
            "https://github.com/flite/mock-aws-java-sdk";

    public static final String QUEUE_URL_PREFIX = "https://mock.flite.com/mockaws/";
    public static final String MARKER_CLIENT_EXCEPTION = "mock-aws-client-exception";
    public static final String QUEUE_URL_CLIENT_EXCEPTION = QUEUE_URL_PREFIX + MARKER_CLIENT_EXCEPTION;
    public static final String MARKER_SERVICE_EXCEPTION = "mock-aws-service-exception";
    public static final String QUEUE_URL_SERVICE_EXCEPTION = QUEUE_URL_PREFIX + MARKER_SERVICE_EXCEPTION;
    public static final String MESSAGE_ID_PREFIX = "mock-aws-message-id-";
    public static final String RECEIPT_ID_PREFIX = "mock-aws-receipt-id-";

    private AtomicLong incrementer = new AtomicLong(System.currentTimeMillis() * 1000);
    private Map<String, List<Message>> allQueues = new ConcurrentHashMap<String, List<Message>>();
    private Map<String, Map<String, Message>> retrievedMessages = new ConcurrentHashMap<String, Map<String,Message>>();

    //@Override
    public GetQueueUrlResult getQueueUrl(final GetQueueUrlRequest request) throws AmazonServiceException, AmazonClientException {
        if (request == null) { throw new AmazonClientException("Null GetQueueUrlRequest"); }
        final String queueName = request.getQueueName();
        final String queueUrl = QUEUE_URL_PREFIX + queueName;
        checkURLForException(queueUrl);
        // Per documentation, supposedly throws QueueDoesNotExistException,
        // but in my tests, they actually just throw AmazonServiceException
        if (!allQueues.containsKey(queueUrl)) { throw new AmazonServiceException("Queue Not Found: " + queueUrl); }
        return new GetQueueUrlResult().withQueueUrl(queueUrl);
    }

    //@Override
    public CreateQueueResult createQueue(final CreateQueueRequest request) throws AmazonServiceException, AmazonClientException {
        if (request == null) { throw new AmazonClientException("Null CreateQueueRequest"); }
        final String queueName = request.getQueueName();
        if (StringUtils.isBlank(queueName) || queueName.length() > 80) { throw new AmazonServiceException("Invalid queue name: " + queueName); }
        final String queueUrl = QUEUE_URL_PREFIX + queueName;
        checkURLForException(queueUrl);
        // Per documentation, throws QueueNameExistsException, but in my testing, they actually
        // just quietly return the CreateQueueResult
        // (Also note: we are ignoring the documented exception: QueueDeletedRecentlyException)
        if (!allQueues.containsKey(queueUrl)) {
            allQueues.put(queueUrl, Collections.synchronizedList(new ArrayList<Message>()));
            retrievedMessages.put(queueUrl, new ConcurrentHashMap<String, Message>());
        }
        return new CreateQueueResult().withQueueUrl(queueUrl);
    }

    //@Override
    public SendMessageResult sendMessage(final SendMessageRequest request) throws AmazonServiceException, AmazonClientException {
        if (request == null) { throw new AmazonClientException("Null SendMessageRequest"); }
        final String queueUrl = request.getQueueUrl();
        checkURLForException(queueUrl);
        checkStringForExceptionMarker(request.getMessageBody());
        // Ignoring the following exception: InvalidMessageContentsException (thrown for character set conditions?)
        if (!allQueues.containsKey(queueUrl)) { throw new AmazonServiceException("Queue Not Found: " + queueUrl); }
        final Long seq = incrementer.getAndIncrement();
        final Message msg = new Message()
                .withMessageAttributes(request.getMessageAttributes())
                .withBody(StringUtils.defaultString(request.getMessageBody()))
                .withMD5OfBody(makeMD5(request.getMessageBody()))
                .withMessageId(MESSAGE_ID_PREFIX + seq);
        allQueues.get(queueUrl).add(msg);
        return new SendMessageResult().withMD5OfMessageBody(msg.getMD5OfBody()).withMessageId(msg.getMessageId());
    }

    //@Override
    public void deleteMessage(final DeleteMessageRequest request) throws AmazonServiceException, AmazonClientException {
        if (request == null) { throw new AmazonClientException("Null DeleteMessageRequest"); }
        final String queueUrl = request.getQueueUrl();
        checkURLForException(queueUrl);
        checkStringForExceptionMarker(request.getReceiptHandle());
        // Ignoring the documented exception: InvalidIdFormatException
        if (!allQueues.containsKey(queueUrl)) { throw new AmazonServiceException("Queue Not Found: " + queueUrl); }
        if (!retrievedMessages.containsKey(queueUrl)) { throw new AmazonServiceException("Queue Not Found: " + queueUrl); }
        if (!retrievedMessages.get(queueUrl).containsKey(request.getReceiptHandle())) { throw new ReceiptHandleIsInvalidException("Reciept Handle Not Found: " + request.getReceiptHandle()); }
        retrievedMessages.get(queueUrl).remove(request.getReceiptHandle());
    }

    //@Override
    public ReceiveMessageResult receiveMessage(final ReceiveMessageRequest request) throws AmazonServiceException, AmazonClientException {
        if (request == null) { throw new AmazonClientException("Null ReceiveMessageRequest"); }
        final String queueUrl = request.getQueueUrl();
        checkURLForException(queueUrl);
        // Per documentation throws OverLimitException, but in my testing,
        // they actually only throw AmazonServiceException
        final Integer max = request.getMaxNumberOfMessages();
        if (max == null || max < 1 || max > 10) { throw new AmazonServiceException("MaxNumberOfMessages must be a value between [1,10]"); }
        final ReceiveMessageResult result = new ReceiveMessageResult();
        int received = 0;
        boolean avail = true;
        while (received < request.getMaxNumberOfMessages() && avail) {
            try {
                final Message msg = allQueues.get(queueUrl).remove(0);
                received++;
                msg.setReceiptHandle(RECEIPT_ID_PREFIX + incrementer.getAndIncrement());
                retrievedMessages.get(queueUrl).put(msg.getReceiptHandle(), msg);
                result.withMessages(msg);
            } catch (Exception ex) {
                avail = false;
            }
        }
        return result;
    }

    //@Override
    public ListQueuesResult listQueues(final ListQueuesRequest request) throws AmazonServiceException, AmazonClientException {
        if (request == null) { throw new AmazonClientException("Null ListQueuesRequest"); }
        checkStringForExceptionMarker(request.getQueueNamePrefix());

        final String effectivePrefix = QUEUE_URL_PREFIX + (request.getQueueNamePrefix() == null ? "" : request.getQueueNamePrefix());
        final ListQueuesResult result = new ListQueuesResult();
        for (final String url : allQueues.keySet()) {
            if (StringUtils.startsWith(url, effectivePrefix)) {
                result.withQueueUrls(url);
            }
        }
        return result;
    }

    //@Override
    public ListQueuesResult listQueues() throws AmazonServiceException, AmazonClientException {
        return listQueues(new ListQueuesRequest(""));
    }

    //@Override
    public void deleteQueue(final DeleteQueueRequest request) throws AmazonServiceException, AmazonClientException {
        if (request == null) { throw new AmazonClientException("Null DeleteQueueRequest"); }
        final String queueUrl = request.getQueueUrl();
        checkURLForException(queueUrl);
        if (!allQueues.containsKey(queueUrl)) { throw new AmazonServiceException("Queue Not Found: " + queueUrl); }
        allQueues.remove(queueUrl);
        if (retrievedMessages.containsKey(queueUrl)) { retrievedMessages.remove(queueUrl); }
    }

    public static final String ALL = "All";
    public static final String NUM_MSGS = "ApproximateNumberOfMessages";
    public static final String NUM_NOT_VISIBLE = "ApproximateNumberOfMessagesNotVisible";
    public static final String VIS_TIMEOUT = "VisibilityTimeout";
    public static final String CREATED_TIMESTAMP = "CreatedTimestamp";
    public static final String MODIFIED_TIMESTAMP = "LastModifiedTimestamp";
    public static final String POLICY = "Policy";
    public static final String MAX_SIZE = "MaximumMessageSize";
    public static final String RETENTION = "MessageRetentionPeriod";
    public static final String ARN = "QueueArn";
    public static final String MSGS_DELAYED = "ApproximateNumberOfMessagesDelayed";
    public static final String DELAY_SEC = "DelaySeconds";
    private static final List<String> attbs = Arrays.asList(ALL, NUM_MSGS, NUM_NOT_VISIBLE, VIS_TIMEOUT, CREATED_TIMESTAMP,
            MODIFIED_TIMESTAMP, POLICY, MAX_SIZE, RETENTION, ARN, MSGS_DELAYED, DELAY_SEC);
    //@Override
    public GetQueueAttributesResult getQueueAttributes(final GetQueueAttributesRequest request) throws AmazonServiceException, AmazonClientException {
        if (request == null) { throw new AmazonClientException("Null GetQueueAttributesRequest"); }
        final String queueUrl = request.getQueueUrl();
        checkURLForException(queueUrl);
        for (final String attb : request.getAttributeNames()) {
            checkStringForExceptionMarker(attb);
            if (!attbs.contains(attb)) { throw new InvalidAttributeNameException("Invalid Attribute Name: " + attb); }
        }

        final Map<String, String> results = new ConcurrentHashMap<String, String>();
        final boolean hasAll = request.getAttributeNames().contains(ALL);
        if (hasAll || request.getAttributeNames().contains(NUM_MSGS)) {
            results.put(NUM_MSGS, allQueues.get(queueUrl) == null ? "0" : allQueues.get(queueUrl).size()+"");
        }
        if (hasAll || request.getAttributeNames().contains(NUM_NOT_VISIBLE)) {
            results.put(NUM_NOT_VISIBLE, retrievedMessages.get(queueUrl) == null ? "0" : retrievedMessages.get(queueUrl).size()+"");
        }
        if (hasAll || request.getAttributeNames().contains(VIS_TIMEOUT)) { throw new RuntimeException(NYI_EXCEPTION); }
        if (hasAll || request.getAttributeNames().contains(CREATED_TIMESTAMP)) { throw new RuntimeException(NYI_EXCEPTION); }
        if (hasAll || request.getAttributeNames().contains(MODIFIED_TIMESTAMP)) { throw new RuntimeException(NYI_EXCEPTION); }
        if (hasAll || request.getAttributeNames().contains(POLICY)) { throw new RuntimeException(NYI_EXCEPTION); }
        if (hasAll || request.getAttributeNames().contains(MAX_SIZE)) { throw new RuntimeException(NYI_EXCEPTION); }
        if (hasAll || request.getAttributeNames().contains(RETENTION)) { throw new RuntimeException(NYI_EXCEPTION); }
        if (hasAll || request.getAttributeNames().contains(ARN)) { throw new RuntimeException(NYI_EXCEPTION); }
        if (hasAll || request.getAttributeNames().contains(MSGS_DELAYED)) { throw new RuntimeException(NYI_EXCEPTION); }
        if (hasAll || request.getAttributeNames().contains(DELAY_SEC)) { throw new RuntimeException(NYI_EXCEPTION); }

        return new GetQueueAttributesResult().withAttributes(results);
    }


    //@Override
    public void setEndpoint(String endpoint) throws IllegalArgumentException { throw new RuntimeException(NYI_EXCEPTION); }

    //@Override
    public void setQueueAttributes(SetQueueAttributesRequest setQueueAttributesRequest) throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);
    }

    //@Override
    public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest changeMessageVisibilityBatchRequest) throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);
    }

    //@Override
    public void changeMessageVisibility(ChangeMessageVisibilityRequest changeMessageVisibilityRequest) throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);
    }

    //@Override
    public void removePermission(RemovePermissionRequest removePermissionRequest) throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);
    }

    //@Override
    public SendMessageBatchResult sendMessageBatch(SendMessageBatchRequest sendMessageBatchRequest) throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);
    }

    //@Override
    public DeleteMessageBatchResult deleteMessageBatch(DeleteMessageBatchRequest deleteMessageBatchRequest) throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);
    }

    //@Override
    public void addPermission(AddPermissionRequest addPermissionRequest) throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);
    }


    //@Override
    public void shutdown() {
        throw new RuntimeException(NYI_EXCEPTION);
    }

    //@Override
    public ResponseMetadata getCachedResponseMetadata(AmazonWebServiceRequest request) {
        throw new RuntimeException(NYI_EXCEPTION);
    }

    private static void checkURLForException(final String queueUrl) {
        checkStringForExceptionMarker(queueUrl);
    }

    private static void checkStringForExceptionMarker(final String str) {
        final String src = StringUtils.isBlank(str) ? "" : str.toLowerCase();
        if (src.contains(MARKER_CLIENT_EXCEPTION)) {
            throw new AmazonClientException("Forced AmazonClientException");
        }
        if (src.contains(MARKER_SERVICE_EXCEPTION)) {
            throw new AmazonServiceException("Forced AmazonServiceException");
        }
    }

    private static String makeMD5(final String input) {
        try {
            return new String(Md5Utils.computeMD5Hash(StringUtils.defaultString(input).getBytes()));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setRegion(Region region) throws IllegalArgumentException {
    }

    public void purgeQueue(PurgeQueueRequest purgeQueueRequest)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public ListDeadLetterSourceQueuesResult listDeadLetterSourceQueues(
            ListDeadLetterSourceQueuesRequest listDeadLetterSourceQueuesRequest)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public void setQueueAttributes(String queueUrl,
            Map<String, String> attributes) throws AmazonServiceException,
            AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public ChangeMessageVisibilityBatchResult changeMessageVisibilityBatch(
            String queueUrl,
            List<ChangeMessageVisibilityBatchRequestEntry> entries)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public void changeMessageVisibility(String queueUrl, String receiptHandle,
            Integer visibilityTimeout) throws AmazonServiceException,
            AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public GetQueueUrlResult getQueueUrl(String queueName)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public void removePermission(String queueUrl, String label)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public GetQueueAttributesResult getQueueAttributes(String queueUrl,
            List<String> attributeNames) throws AmazonServiceException,
            AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public SendMessageBatchResult sendMessageBatch(String queueUrl,
            List<SendMessageBatchRequestEntry> entries)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public void deleteQueue(String queueUrl) throws AmazonServiceException,
            AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public SendMessageResult sendMessage(String queueUrl, String messageBody)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public ReceiveMessageResult receiveMessage(String queueUrl)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public ListQueuesResult listQueues(String queueNamePrefix)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public DeleteMessageBatchResult deleteMessageBatch(String queueUrl,
            List<DeleteMessageBatchRequestEntry> entries)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public CreateQueueResult createQueue(String queueName)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);

    }

    public void addPermission(String queueUrl, String label,
            List<String> aWSAccountIds, List<String> actions)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);
        
    }

    public void deleteMessage(String queueUrl, String receiptHandle)
            throws AmazonServiceException, AmazonClientException {
        throw new RuntimeException(NYI_EXCEPTION);
        
    }

}

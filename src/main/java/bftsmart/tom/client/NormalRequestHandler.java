package bftsmart.tom.client;

import bftsmart.tom.core.messages.TOMMessage;
import bftsmart.tom.core.messages.TOMMessageType;
import bftsmart.tom.util.Extractor;
import bftsmart.tom.util.ServiceContent;

import java.util.*;

public class NormalRequestHandler extends AbstractRequestHandler {
	private final Comparator<ServiceContent> comparator;
	private final Extractor responseExtractor;

	public NormalRequestHandler(int me, int session, int sequenceId, int operationId, int viewId,
								TOMMessageType requestType, int timeout, int[] replicas,
								int replyQuorumSize, Comparator<ServiceContent> comparator,
								Extractor responseExtractor) {
		super(me, session, sequenceId, operationId, viewId, requestType, timeout, replicas, replyQuorumSize);
		this.comparator = comparator;
		this.responseExtractor = responseExtractor;
	}

	@Override
	public TOMMessage createRequest(byte[] request, boolean hasReplicaSpecificContent, byte metadata) {
		return new TOMMessage(me, session, sequenceId, operationId, request, hasReplicaSpecificContent,
				metadata, viewId, requestType);
	}

	@Override
	public void processReply(TOMMessage reply, int lastReceivedIndex) {
		//optimization - compare responses after having a quorum of replies
		if (replySenders.size() < replyQuorumSize) {
			return;
		}

		int sameContent = 0;

		logger.debug("Comparing {} responses with response from {}", replySenders.size(), reply.getSender());
		for (TOMMessage msg : replies) {
			if (msg == null)
				continue;
			if (comparator.compare(msg.getContent(), reply.getContent()) == 0) {
				sameContent++;
				if (sameContent >= replyQuorumSize) {
					response = responseExtractor.extractResponse(replies, sameContent, lastReceivedIndex);
					response.setViewID(reply.getViewID());
					semaphore.release();
					return;
				}
			}
		}

		if (replySenders.size() == replicas.length) {
			semaphore.release();
		}

	}

	@Override
	public void printState() {

	}
}

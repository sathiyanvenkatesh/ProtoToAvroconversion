package com.sample.testprototoavro;

import com.example.tutorial.RealTimeBiddingProtos;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * @author Sathiyan
 */
public class ProtoBufMsgQueue {
     public static LinkedBlockingQueue<RealTimeBiddingProtos.BidRequest> ProtoQueue = new LinkedBlockingQueue<RealTimeBiddingProtos.BidRequest>();
    
}
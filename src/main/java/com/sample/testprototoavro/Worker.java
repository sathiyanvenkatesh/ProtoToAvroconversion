package com.sample.testprototoavro;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.avro.protobuf.ProtobufDatumReader;
import org.apache.avro.protobuf.ProtobufDatumWriter;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.springframework.util.SystemPropertyUtils;

import com.demo.sampleTest;
import com.example.tutorial.RealTimeBiddingProtos;
import com.example.tutorial.RealTimeBiddingProtos.BidRequest;
import com.google.openbidder.containers.NativeRtbContainer;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;


public class Worker implements Runnable {
	public boolean running = false;

	String bidrequest_container = null;

	public String getBidrequest_container() {
		return bidrequest_container;
	}

	public void setBidrequest_container(String bidrequest_container) {
		this.bidrequest_container = bidrequest_container;
	}

	public Worker() {
		Thread thread = new Thread(this);
		thread.start();
	}

	public static void main(String[] args) throws InterruptedException {
		List<Worker> workers = new ArrayList<Worker>();

		System.out.println("This is currently running on the main thread, " + "the id is: " + Thread.currentThread().getId());

		Date start = new Date();

		// start 5 workers
		for (int i = 0; i <10; i++) {
			workers.add(new Worker());
		}

		// We must force the main thread to wait for all the workers
		// to finish their work before we check to see how long it
		// took to complete
		for (Worker worker : workers) {
			while (worker.running) {
				Thread.sleep(100);
			}
		}

		Date end = new Date();

		long difference = end.getTime() - start.getTime();

		System.out.println("This whole process took: " + difference / 1000 + " seconds.");
	}

	// @Override
	public void run() {
		this.running = true;		
		 RealTimeBiddingProtos.BidRequest bidreq = null;
		 RealTimeBiddingProtos.BidRequest bidreq2 = null;
		 NativeRtbContainer.RequestContainer bdreq1=null;
		if(bidrequest_container!=null){
			 byte[] key_b = sampleTest.hexStringToByteArray(bidrequest_container);
            try { 
            	bdreq1=NativeRtbContainer.RequestContainer.parseFrom(key_b);
                bidreq = RealTimeBiddingProtos.BidRequest.parseFrom(bdreq1.getRequest());
                ProtoBufMsgQueue.ProtoQueue.offer(bidreq,100L, TimeUnit.MILLISECONDS);
                //System.out.println("ProtoBufMsgQueue"+ProtoBufMsgQueue.ProtoQueue.size());
           } catch (Exception e) {
           }          
       }
		
		
		
			
				 try {
					 RealTimeBiddingProtos.BidRequest bidreqconbyte1 =ProtoBufMsgQueue.ProtoQueue.take(); 
					 System.out.println("size"+ProtoBufMsgQueue.ProtoQueue.size());
					 bidreq2 = RealTimeBiddingProtos.BidRequest.parseFrom(bidreqconbyte1.toByteArray());
		             //System.out.println("bidreq...."+bidreq.getSerializedSize());
					
				} catch (InvalidProtocolBufferException e) {
					e.printStackTrace();
				} catch (InterruptedException e1) {					
					e1.printStackTrace();
				}
			 	    ProtobufDatumWriter<RealTimeBiddingProtos.BidRequest> pbWriter = new ProtobufDatumWriter<RealTimeBiddingProtos.BidRequest>(RealTimeBiddingProtos.BidRequest.class);
			 	  
					DataFileWriter<RealTimeBiddingProtos.BidRequest> dataFileWriter = new DataFileWriter<RealTimeBiddingProtos.BidRequest>(pbWriter).setSyncInterval(21097);
			        Schema schema= ProtobufData.get().getSchema(RealTimeBiddingProtos.BidRequest.class);
			        //String fileName = new SimpleDateFormat("yyyyMMddhhmmssSSS'.avro'").format(new Date());
			        String path = "D:" +File.separator +"\\"+"logs" +File.separator +"\\"+  new SimpleDateFormat("yyyyMMddhhmmssSSSSSS'.avro'").format(new Date());
			      //String path = "/usr" +File.separator +"/"+"logs" +File.separator +"/"+  new SimpleDateFormat("yyyyMMddhhmmssSSSSSS'.avro'").format(new Date());
			        try {
			        	synchronized (dataFileWriter){
						dataFileWriter.create(schema, new File(path));
						//synchronized (dataFileWriter){
						dataFileWriter.append(bidreq2);
						//dataFileWriter.
						//dataFileWriter.appendTo(new File(path));
						//}
						dataFileWriter.close();	
			        	}
					} 
			        catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

			        
		this.running = true;
	}	

}
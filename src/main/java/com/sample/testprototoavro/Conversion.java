package com.sample.testprototoavro;

import java.io.ByteArrayOutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.avro.reflect.ReflectDatumWriter;

import com.demo.sampleTest;
import com.example.tutorial.RealTimeBiddingProtos;
import com.google.openbidder.containers.NativeRtbContainer;

public class Conversion {
	String bidrequest_container = null;

	public String getBidrequest_container() {
		return bidrequest_container;
	}

	public void setBidrequest_container(String bidrequest_container) {
		this.bidrequest_container = bidrequest_container;
	}
	
	 RealTimeBiddingProtos.BidRequest objectToSerialize=null;
	 RealTimeBiddingProtos.BidRequest bidreq = null;
	 RealTimeBiddingProtos.BidRequest bidreq2 = null;
	 NativeRtbContainer.RequestContainer bdreq1=null;
	public  void putdata(){ 
	if(bidrequest_container!=null){
		 byte[] key_b = sampleTest.hexStringToByteArray(bidrequest_container);
       try { 
       	bdreq1=NativeRtbContainer.RequestContainer.parseFrom(key_b);
           bidreq = RealTimeBiddingProtos.BidRequest.parseFrom(bdreq1.getRequest());
           ProtoBufMsgQueue.ProtoQueue.offer(bidreq,100L, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
      }          
  }}
	
	   public byte[] serialize(RealTimeBiddingProtos.BidRequest objectToSerialize) {
		   ByteArrayOutputStream out = new ByteArrayOutputStream();
		   Schema schema= ProtobufData.get().getSchema(RealTimeBiddingProtos.BidRequest.class);
		   DatumWriter<RealTimeBiddingProtos.BidRequest> reflectDatumWriter = new ReflectDatumWriter<RealTimeBiddingProtos.BidRequest>(schema);
		   DataFileWriter<RealTimeBiddingProtos.BidRequest> fileWriter = null;
		   try {
		       fileWriter = new DataFileWriter<RealTimeBiddingProtos.BidRequest>(reflectDatumWriter);
		       fileWriter.setCodec(CodecFactory.snappyCodec());
		       fileWriter.create(schema, out);
		       fileWriter.append(objectToSerialize);
		       fileWriter.close();
		   } catch (Exception e) {
		       System.out.println(e);
		       return null;
		   }
		   return out.toByteArray();


}
}

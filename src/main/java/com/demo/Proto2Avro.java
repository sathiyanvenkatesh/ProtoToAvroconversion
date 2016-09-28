package com.demo;

import java.io.IOException;

import org.apache.avro.protobuf.ProtobufDatumReader;

import com.example.tutorial.RealTimeBiddingProtos;

public class Proto2Avro {
	 public static void main(String[] args) throws IOException {
	        System.out.println("******************************************************************************");
	        System.out.println("******** How to convert a proto schema to avro with 2 lines of code **********");
	        System.out.println("******************************************************************************");

	        System.out.println("protobuff schema description:");
	        System.out.println(RealTimeBiddingProtos.BidRequest.getDescriptor().toProto());
	        System.out.println("----------");
	        System.out.println("Avro schema:");
	        ProtobufDatumReader<RealTimeBiddingProtos.BidRequest> datumReader = new ProtobufDatumReader<RealTimeBiddingProtos.BidRequest>(RealTimeBiddingProtos.BidRequest.class);
	        System.out.println(datumReader.getSchema());



	    }

}

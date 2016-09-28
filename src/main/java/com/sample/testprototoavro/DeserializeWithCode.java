package com.sample.testprototoavro;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import com.example.tutorial.RealTimeBiddingProtos;


public class DeserializeWithCode {
	public static void main(String[] args) throws IOException {
//		//String path = "D:" +File.separator +"\\"+"logs" +File.separator +"\\"+  new SimpleDateFormat("yyyyMMddhhmmssSSSSSS'.avro'").format(new Date());
//		File file = new File("20160725115550000089.avro");		
//		DatumReader<RealTimeBiddingProtos.BidRequest> userDatumReader = new SpecificDatumReader<RealTimeBiddingProtos.BidRequest>(RealTimeBiddingProtos.BidRequest.class);
//		DataFileReader<RealTimeBiddingProtos.BidRequest> dataFileReader = new DataFileReader<RealTimeBiddingProtos.BidRequest>(file, userDatumReader);
//		RealTimeBiddingProtos.BidRequest req = null;
//		while (dataFileReader.hasNext()) 
//		{
//			req = dataFileReader.next();			
//			System.out.println(req);
//		}
		
		//public static void main(String[] args) throws IOException {
			Schema schema = new Schema.Parser().parse(new File("BiddReq.avsc"));
			GenericRecord emp = new GenericData.Record(schema);
			
			File file = new File("D:/logs/20160928042324793.avro");
			
			DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
			DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);

			while (dataFileReader.hasNext()) 
			{
			emp = dataFileReader.next();
			System.out.println("data"+emp);
			}
		//}
	}
}
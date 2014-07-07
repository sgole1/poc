package com.mkyong.core;

import java.net.UnknownHostException;
import java.util.Date;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	try {
			MongoClient mongo = new MongoClient("ec2-54-81-150-163.compute-1.amazonaws.com", 27017);
			DB db = mongo.getDB("blocksDB");
			DBCollection table = db.getCollection("blocksCollection");
//			BasicDBObject document = createCollectionObject();
//			table.insert(document);
			//
			
			DBCursor cursor = table.find().limit(10);
			while(cursor.hasNext()){
				BasicDBObject obj = (BasicDBObject) cursor.next();
				obj.get("blocks");
			}
		 
			//DBCursor cursor = table.find(searchQuery);
		 
			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

	private static BasicDBObject createCollectionObject() {
		BasicDBObject document = new BasicDBObject();
		document.put("name", "mkyong");
		document.put("age", 30);
		document.put("createdDate", new Date());
		return document;
	}
}

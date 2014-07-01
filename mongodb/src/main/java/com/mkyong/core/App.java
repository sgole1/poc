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
			MongoClient mongo = new MongoClient("localhost", 27017);
			DB db = mongo.getDB("testdb");
			DBCollection table = db.getCollection("user");
			BasicDBObject document = createCollectionObject();
			table.insert(document);
			//
			BasicDBObject searchQuery = new BasicDBObject();
			searchQuery.put("name", "mkyong");
		 
			DBCursor cursor = table.find(searchQuery);
		 
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

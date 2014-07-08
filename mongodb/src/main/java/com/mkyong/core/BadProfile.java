package com.mkyong.core;

import java.io.File;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * Hello world!
 * 
 */
public class BadProfile {
	public static void main(String[] args) {
		
		Map<Integer, List<Integer>> data = new HashMap<Integer, List<Integer>>();
		String outputFile = "users.csv";
		// before we open the file check to see if it already exists
		boolean alreadyExists = new File(outputFile).exists();

		MongoClient mongo = getConnection();
		DB db = mongo.getDB("blocksDB");
		DBCollection table = db.getCollection("blocksCollection");
		getBadProfiles(table);

	}

	

	private static MongoClient getConnection() {
		MongoClient mongo = null;
		try {
			mongo = new MongoClient("ec2-54-86-54-97.compute-1.amazonaws.com",
					27017);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mongo;
	}

	private static void getBadProfiles(DBCollection table) {
		System.out.println("Start time : "+(System.currentTimeMillis()/1000));
		Long startTime = System.currentTimeMillis()/1000;
		DBCursor profileIds = table.find().limit(1);
		ExecutorService executor = Executors.newFixedThreadPool(10);
		int count = 0;
		while (profileIds.hasNext()) {
			BasicDBObject profile = (BasicDBObject) profileIds.next();
			Profile p = new BadProfile().new Profile(table, profile);
			
			System.out.println("Submitted task"+count++);
				executor.submit(p);
			
			// return getBadEntryPerProfile(table, profile);
		}
		executor.shutdown();
		while (!executor.isTerminated()) {
			
		}
		 System.out.println("Finished all threads");
		 Long endTIme = System.currentTimeMillis()/1000;
		 System.out.println("End time : "+(System.currentTimeMillis()/1000));
		 System.out.println("Total time taken : "+(startTime-endTIme)/60);

	}

	class Profile implements Runnable {
		DBCollection table;
		BasicDBObject profile;

		public Profile(DBCollection table, BasicDBObject profile) {
			this.table = table;
			this.profile = profile;
		}

		@Override
		public void run() {
			System.out.println("request submitted for thread"+Thread.currentThread().getName());
			getBadEntryPerProfile(table, profile);

		}

		/**
		 * @param table
		 * @param profileIds
		 * @return
		 */
		private  ProfileVO getBadEntryPerProfile(DBCollection table,
				BasicDBObject profile) {

			Integer profID = (Integer) profile.get("profID");
			System.out.println("scanned profile id :" + profID);
			ProfileVO profileVO = getAllBlockedOrFavPIDsByProfileId(table,
					profID);
//			ProfileVO profileVO = getAllBlockedOrFavPIDsByProfileId(table,
//					2029785);
			
			setBadBlockedProfileEntries(profile, profileVO);
			setBadFavProfileEntries(profile, profileVO);
			return profileVO;
		}

		/**
		 * @param profile
		 * @param profileVO
		 */
		private  void setBadFavProfileEntries(BasicDBObject profile,
				ProfileVO profileVO) {
			BasicDBList favByObjects = (BasicDBList) profile.get("favorite_by");
			if (profileVO.getFavProfiles() != null) {
				List<Integer> favProfileIds = new ArrayList<Integer>();
				if(favByObjects != null){
					for (Object obj : favByObjects) {
						favProfileIds.add((Integer) obj);
					}
				}
				profileVO.getFavProfiles().removeAll(favProfileIds);
				profileVO.badFavProfiles = profileVO.getFavProfiles();
				System.out.println("Fav By profile ids not entered : "
						+ profileVO.badFavProfiles);
			}
		}

		/**
		 * @param profile
		 * @param profileVO
		 */
		private  void setBadBlockedProfileEntries(BasicDBObject profile,
				ProfileVO profileVO) {
			BasicDBList blockedByObjects = (BasicDBList) profile.get("blocked_by");
			List<Integer> blockedProfileIds = new ArrayList<Integer>();
			if (profileVO.getBlockedProfiles() != null) {
				for (Object obj : blockedByObjects) {
					blockedProfileIds.add((Integer) obj);
				}
			}
				profileVO.getBlockedProfiles().removeAll(blockedProfileIds);
				profileVO.badBlockedProfile = profileVO.getBlockedProfiles();
				System.out.println("Blocked By profile ids not entered : "
						+ profileVO.badBlockedProfile);
			
		}

		private  ProfileVO getAllBlockedOrFavPIDsByProfileId(
				DBCollection table, Integer profileId) {
			BasicDBObject textSearch = new BasicDBObject();
			List<Integer> list = new ArrayList<Integer>();
			list.add(profileId);
			List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
			obj.add(new BasicDBObject("blocks", new BasicDBObject("$in", list)));
			obj.add(new BasicDBObject("favorites", new BasicDBObject("$in", list)));
			textSearch.put("$or", obj);
			DBCursor profileIdsToBeScaned = table.find(textSearch);
			
			List<Integer> favouriteProfileIds = new ArrayList<Integer>();
			List<Integer> blockedProfileIds = new ArrayList<Integer>();

			while (profileIdsToBeScaned.hasNext()) {
				DBObject profile = profileIdsToBeScaned.next();
				BasicDBList blockedByObjects = (BasicDBList) profile
						.get("blocks");
				if (blockedByObjects != null
						&& blockedByObjects.contains(profileId)) {
					blockedProfileIds.add((Integer) profile.get("profID"));
				}
				BasicDBList favByObjects = (BasicDBList) profile
						.get("favorites");
				
				if (favByObjects != null && favByObjects.contains(profileId)) {
					favouriteProfileIds.add((Integer) profile.get("profID"));
				}
			}
			ProfileVO vo = new ProfileVO();
			vo.blockedProfiles = blockedProfileIds;
			vo.favProfiles = favouriteProfileIds;
			vo.profileId = profileId;
			return vo;
		}

	}

	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	//
	// //BasicDBList blockedProfileIds = (BasicDBList) profileId.get("blocks");
	// System.out.println("blockedProfileIds:"+blockedProfileIds);
	// if (blockedProfileIds != null) {
	// for (Object blockedProfileId : blockedProfileIds) {
	// // System.out.println("object::" + obj1);
	// // System.out.print("obj ::::" + obj.get("profID"));
	// BasicDBObject blockedPId = getBlockedProfileIdDetail(
	// table, blockedProfileId);
	// // System.out.print("blockedProfileId  :"+blockedProfileId);
	// // if (blockedProfileIdDetail != null) {
	// boolean badFlag = false;Integer badProfileId = null;
	// // while (blockedProfileIdDetail.hasNext()) {
	// // BasicDBObject blockedPId = (BasicDBObject) blockedProfileIdDetail
	// // .next();
	// //
	// BasicDBList blockedByObjects = (BasicDBList)
	// blockedPId.get("blocked_by");
	//
	// // System.out.println("BlockedBy Profile ids :"+blockedByObjects);
	//
	// if(blockedByObjects != null){
	// for(Object blockedByProfile :blockedByObjects){
	// Integer blockedById = (Integer)blockedByProfile;
	// if(blockedById == profID){
	// badFlag = false;
	// System.out.println("badFlag :"+blockedById);
	// break;
	// }else{
	// badProfileId = blockedById;
	// badFlag = true;
	// }
	// }
	// }
	//
	//
	//
	// if(badFlag){
	// System.out.println("BlockedBy Profile ids :"+badProfileId);
	// List<Integer> blockedProfiles = data.get(profileId.get("profID"));
	// if(blockedProfiles != null){
	// blockedProfiles.add(badProfileId);
	// }else{
	// blockedProfiles = new ArrayList<Integer>();
	// blockedProfiles.add(badProfileId);
	// data.put(profID, blockedProfiles);
	// }
	// // System.out.println("badProfileId::" + badProfileId);
	// // System.out.println("blockedBy::" + obj.get("profID"));
	// }
	// }
	// }
	// }
	// }

	// } catch (UnknownHostException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// System.out.print("TOtal Bad profiles :"+data.size());
	// for(Map.Entry<Integer, List<Integer>> entry : data.entrySet()){
	// System.out.print("Profile Id:"+entry.getKey());
	// System.out.println(" .Bad blocked profiles Id:"+entry.getValue());
	// }
	// }
	//
	// private static BasicDBObject getBlockedProfileIdDetail(DBCollection
	// table,
	// Object blockedProfileId) {
	// BasicDBObject textSearch = new BasicDBObject();
	// textSearch.put("profID", blockedProfileId);
	// DBCursor blockedProfileIdDetail = table.find(textSearch);
	// System.out.print("blockedProfileId  :"+blockedProfileId);
	// if (blockedProfileIdDetail != null) {
	// boolean badFlag = false;Integer badProfileId = null;
	// while (blockedProfileIdDetail.hasNext()) {
	// BasicDBObject blockedPId = (BasicDBObject) blockedProfileIdDetail
	// .next();
	// return blockedPId;
	// }
	// }
	// return null;
	// }
	//
	// }

	class ProfileVO1 {
		List<Integer> badBlockedProfile;
		List<Integer> badFavProfiles;
		List<Integer> blockedProfiles;
		List<Integer> favProfiles;
		Integer profileId;

		public List<Integer> getBadBlockedProfile() {
			return badBlockedProfile;
		}

		public List<Integer> getBadFavProfiles() {
			return badFavProfiles;
		}

		public List<Integer> getBlockedProfiles() {
			return blockedProfiles;
		}

		public List<Integer> getFavProfiles() {
			return favProfiles;
		}

	}
}
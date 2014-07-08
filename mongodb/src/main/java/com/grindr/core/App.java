package com.grindr.core;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

public class App {
	public static void main(String[] args) {
		Map<Integer, List<Integer>> data = new HashMap<Integer, List<Integer>>();

		MongoClient mongo = getConnection();
		DB db = mongo.getDB("blocksDB");
		DBCollection table = db.getCollection("blocksCollection");
		//getBadProfiles(table);

	}

	private  ProfileVO getBadProfiles(DBCollection table) {
		DBCursor profileIds = table.find().limit(4);
		while (profileIds.hasNext()) {
			BasicDBObject profile = (BasicDBObject) profileIds.next();
			return getBadEntryPerProfile(table, profile);
		}
		return null;
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
		BasicDBList favByObjects = (BasicDBList) profile.get("favorites");
		if (favByObjects != null) {
			List<Integer> favProfileIds = new ArrayList<Integer>();
			for (Object obj : favByObjects) {
				favProfileIds.add((Integer) obj);
			}
			favProfileIds.removeAll(profileVO.getBlockedProfiles());
			profileVO.badFavProfiles = favProfileIds;
			System.out.println("Fav profile ids not entered : "
					+ profileVO.badFavProfiles);
		}
	}

	/**
	 * @param profile
	 * @param profileVO
	 */
	private static void setBadBlockedProfileEntries(BasicDBObject profile,
			ProfileVO profileVO) {
		BasicDBList blockedByObjects = (BasicDBList) profile.get("blocks");
		List<Integer> blockedProfileIds = new ArrayList<Integer>();
		if (blockedByObjects != null) {
			for (Object obj : blockedByObjects) {
				blockedProfileIds.add((Integer) obj);
			}
			blockedProfileIds.removeAll(profileVO.getBlockedProfiles());
			profileVO.badFavProfiles = blockedProfileIds;
			System.out.println("Blocked profile ids not entered : "
					+ profileVO.badBlockedProfile);
		}
	}

	private  ProfileVO getAllBlockedOrFavPIDsByProfileId(
			DBCollection table, Integer profileId) {
		BasicDBObject textSearch = new BasicDBObject();
		List<Integer> list = new ArrayList<Integer>();
		list.add(profileId);
		textSearch.put("blocked_by", new BasicDBObject("$in", list));
		DBCursor profileIdsToBeScaned = table.find(textSearch);
		List<Integer> favouriteProfileIds = new ArrayList<Integer>();
		List<Integer> blockedProfileIds = new ArrayList<Integer>();

		while (profileIdsToBeScaned.hasNext()) {
			DBObject profile = profileIdsToBeScaned.next();
			BasicDBList blockedByObjects = (BasicDBList) profile
					.get("blocked_by");
			BasicDBList favByObjects = (BasicDBList) profile.get("favorite_by");
			if (blockedByObjects != null
					&& blockedByObjects.contains(profileId)) {
				blockedProfileIds.add((Integer) profile.get("profID"));
			}
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
// BasicDBList blockedByObjects = (BasicDBList) blockedPId.get("blocked_by");
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
// private static BasicDBObject getBlockedProfileIdDetail(DBCollection table,
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

class ProfileVO {
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
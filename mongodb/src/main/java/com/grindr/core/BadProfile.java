package com.grindr.core;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoOptions;

/**
 * Hello world!
 * 
 */
/**
 * @author Sumit
 * 
 */
public class BadProfile {
	private static int MAX_THREAD_COUNT = 10;
	private static int MAX_RECORDS_TOBE_PROCESS = 100;
	private static String DATABASE_HOSTNAME = "ec2-54-86-54-97.compute-1.amazonaws.com";
	private static PrintWriter csvOutput = null;

	public static void main(String[] args) {

		if (args.length == 3) {
			DATABASE_HOSTNAME = args[0];
			MAX_THREAD_COUNT = new Integer(args[1]);
			MAX_RECORDS_TOBE_PROCESS = new Integer(args[2]);
		}
		MongoClient mongoClient = null;

		try {
			initFileWriter();
			mongoClient = getConnection();
			DB db = mongoClient.getDB("blocksDB");
			DBCollection table = db.getCollection("blocksCollection");
			getBadProfiles(table);
		} catch (Exception e) {
			e.printStackTrace();

		} finally {
			csvOutput.close();
		}
	}

	/**
	 * Create file
	 */
	private static void initFileWriter() {
		String outputFile = "Bad_Profiles_Data" + System.currentTimeMillis()
				+ ".csv";
		// before we open the file check to see if it already exists

		try {
			csvOutput = new PrintWriter(new File(outputFile));
			csvOutput.write("Id");
			csvOutput.write("\t");
			csvOutput.write("Missing Blocked-By");
			csvOutput.write("\t");
			csvOutput.write("Missing Fav-By");
			csvOutput.write("\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Get mongodb connection
	 */
	private static MongoClient getConnection() {

		MongoClient mongo = null;

		MongoClientOptions option = new MongoClientOptions.Builder()
				.autoConnectRetry(true).connectTimeout(999999999).socketKeepAlive(true).build();
		try {
			mongo = new MongoClient(DATABASE_HOSTNAME, option);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mongo;
	}

	/**
	 * 
	 * Extract bad profiles
	 * 
	 * @param table
	 *            : collection for which we want to extract the bad data
	 */

	private static void getBadProfiles(DBCollection table) {
		System.out.println("Start time : "
				+ (System.currentTimeMillis() / 1000));
		Long startTime = System.currentTimeMillis() / 1000;
		DBCursor profileIds = null;
		
		if(MAX_RECORDS_TOBE_PROCESS == 0){
			profileIds = table.find();
		}else{
			profileIds = table.find().limit(MAX_RECORDS_TOBE_PROCESS);
		}
		
		ExecutorService executor = Executors
				.newFixedThreadPool(MAX_THREAD_COUNT);
		int count =0;
		try {
			while (profileIds.hasNext()) {
				//System.out.println("Total Profiles in process/processed::"+count++);
				BasicDBObject profile = (BasicDBObject) profileIds.next();
				Profile p = new BadProfile().new Profile(table, profile);
				executor.submit(p);
			}
		} finally {
			profileIds.close();
		}
		executor.shutdown();
		while (!executor.isTerminated()) {

		}
		System.out.println("Finished all threads");
		Long endTime = System.currentTimeMillis() / 1000;
		System.out.println("End time : " + (System.currentTimeMillis() / 1000));
		System.out.println("Total time taken : " + (startTime - endTime) / 60);

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
			
//			System.out.println("request submitted for thread"
//					+ Thread.currentThread().getName());
			try {
				getBadEntryPerProfile(table, profile);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		/**
		 * To get the bad profiles
		 * 
		 * @param table
		 * @param profileIds
		 * @return
		 */
		private ProfileVO1 getBadEntryPerProfile(DBCollection table,
				BasicDBObject profile) {

			Integer profID = (Integer) profile.get("profID");
			// System.out.println("scanned profile id :" + profID);
			ProfileVO1 profileVO = getAllBlockedOrFavPIDsByProfileId(table,
					profID);
			if (profileVO != null) {
				setBadBlockedProfileEntries(profile, profileVO);
				setBadFavProfileEntries(profile, profileVO);
				writeBadDataInFile(profileVO);
			}
			return profileVO;
		}

		/**
		 * Write the Bad record in the file
		 * 
		 * @param profileVO
		 */
		private void writeBadDataInFile(ProfileVO1 profileVO) {

			csvOutput.write(profileVO.profileId.toString());
			StringBuilder badBlockedPIds = new StringBuilder();

			for (Integer badBProfile : profileVO.badBlockedProfile) {
				badBlockedPIds = badBlockedPIds.append(badBProfile.toString())
						.append(",");
			}

			csvOutput.write("\t");
			csvOutput.write(badBlockedPIds.toString());
			StringBuilder badFavPIds = new StringBuilder();

			for (Integer badFavProfile : profileVO.badFavProfiles) {
				badFavPIds = badFavPIds.append(badFavProfile.toString())
						.append(",");
			}

			csvOutput.write("\t");
			csvOutput.write(badFavPIds.toString());
			csvOutput.write("\n");

		}

		/**
		 * @param profile
		 * @param profileVO
		 */
		private void setBadFavProfileEntries(BasicDBObject profile,
				ProfileVO1 profileVO) {
			BasicDBList favByObjects = (BasicDBList) profile.get("favorite_by");
			if (profileVO.getFavProfiles() != null) {
				List<Integer> favProfileIds = new ArrayList<Integer>();
				if (favByObjects != null) {
					for (Object obj : favByObjects) {
						favProfileIds.add((Integer) obj);
					}
				}
				profileVO.getFavProfiles().removeAll(favProfileIds);
				profileVO.badFavProfiles = profileVO.getFavProfiles();
				// System.out.println("Fav By profile ids not entered : "
				// + profileVO.badFavProfiles);
			}
		}

		/**
		 * @param profile
		 * @param profileVO
		 */
		private void setBadBlockedProfileEntries(BasicDBObject profile,
				ProfileVO1 profileVO) {
			BasicDBList blockedByObjects = (BasicDBList) profile
					.get("blocked_by");
			List<Integer> blockedProfileIds = new ArrayList<Integer>();
			if (blockedByObjects != null) {
				for (Object obj : blockedByObjects) {
					blockedProfileIds.add((Integer) obj);
				}
			}
			profileVO.getBlockedProfiles().removeAll(blockedProfileIds);
			profileVO.badBlockedProfile = profileVO.getBlockedProfiles();
			// System.out.println("Blocked By profile ids not entered : "
			// + profileVO.badBlockedProfile);

		}

		private ProfileVO1 getAllBlockedOrFavPIDsByProfileId(
				DBCollection table, Integer profileId) {
			BasicDBObject textSearch = new BasicDBObject();
			List<Integer> list = new ArrayList<Integer>();
			list.add(profileId);
			List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
			obj.add(new BasicDBObject("blocks", new BasicDBObject("$in", list)));
			obj.add(new BasicDBObject("favorites", new BasicDBObject("$in",
					list)));
			textSearch.put("$or", obj);
			DBCursor profileIdsToBeScaned = table.find(textSearch);
			List<Integer> favouriteProfileIds = new ArrayList<Integer>();
			List<Integer> blockedProfileIds = new ArrayList<Integer>();
			try {
				if (profileIdsToBeScaned != null) {
					while (profileIdsToBeScaned.hasNext()) {
						DBObject profile = profileIdsToBeScaned.next();
						setBlockedProfiles(profileId, blockedProfileIds,
								profile);
						setFavProfiles(profileId, favouriteProfileIds, profile);
					}
				}
				ProfileVO1 vo = new ProfileVO1();
				if (blockedProfileIds.size() > 0
						|| favouriteProfileIds.size() > 0) {
					vo.blockedProfiles = blockedProfileIds;
					vo.favProfiles = favouriteProfileIds;
					vo.profileId = profileId;
					return vo;
				}
			} finally {
				profileIdsToBeScaned.close();
			}

			return null;
		}

		/**
		 * @param profileId
		 * @param favouriteProfileIds
		 * @param profile
		 */
		private void setFavProfiles(Integer profileId,
				List<Integer> favouriteProfileIds, DBObject profile) {
			BasicDBList favByObjects = (BasicDBList) profile.get("favorites");

			if (favByObjects != null && favByObjects.contains(profileId)) {
				favouriteProfileIds.add((Integer) profile.get("profID"));
			}
		}

		/**
		 * @param profileId
		 * @param blockedProfileIds
		 * @param profile
		 */
		private void setBlockedProfiles(Integer profileId,
				List<Integer> blockedProfileIds, DBObject profile) {
			BasicDBList blockedByObjects = (BasicDBList) profile.get("blocks");
			if (blockedByObjects != null
					&& blockedByObjects.contains(profileId)) {
				blockedProfileIds.add((Integer) profile.get("profID"));
			}
		}

	}

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
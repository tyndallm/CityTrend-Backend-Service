import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;

import collections.Hits;
import collections.Venues;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.google.code.morphia.query.Query;
import com.google.code.morphia.query.UpdateOperations;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoURI;
import com.restfb.DefaultFacebookClient;
import com.restfb.DefaultJsonMapper;
import com.restfb.FacebookClient;
import com.restfb.JsonMapper;
import com.restfb.json.JsonObject;
import com.restfb.types.Page;

import daos.VenueDao;
import dtos.FacebookObj;
import dtos.FoursquareObj;
import dtos.HitDto;
import dtos.VenueDto;
import fi.foyt.foursquare.api.FoursquareApi;
import fi.foyt.foursquare.api.FoursquareApiException;
import fi.foyt.foursquare.api.Result;
import fi.foyt.foursquare.api.entities.CompleteVenue;

/*****************************************************************************
*  This class is a back-end process scheduled to run automatically on Heroku. t
*  It pulls existing data from a Mongo database and finds new data from Facebook and Foursquare utilizing their API's.
*  It then calculates the most popular places in a city based on the current data.
****************************************************************************/

public class TrendCalculator
{
    private final static String uriString = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";  // URI removed for public repo
    private final static String foursquareClientId = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";  // ID removed for public repo
	private final static String foursquareClientSecret = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";  // Secret removed for public repo
    private final static String oAuthKey = "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"; // Key removed for public repo

    private Datastore datastore;
	private VenueDao venueDao;
	private Venues venues;
	
	/** venue categories **/
	private final Integer FASTFOOD = 1;
	private final Integer RESTAURANT = 2;
	private final Integer BAR = 3;
	private final Integer CLUB = 4;
	private final Integer ENTERTAINMENT = 5;
	private final Integer STORE = 6;
	private final Integer MUSEUM = 7;
	private final Integer UNCATEGORIZED = 0;
	private final Integer CAFE = 8;
	
	/** hit lifetimes **/
	private final long FASTFOOD_HIT = 600000L;
	private final long CAFE_HIT = 1800000L;
	private final long RESTAURANT_HIT = 4500000L;
	private final long BAR_HIT = 6300000L;
	private final long CLUB_HIT = 6300000L;
	private final long ENTERTAINMENT_HIT = 9000000L;
	private final long STORE_HIT = 1500000L;
	private final long MUSEUM_HIT = 9000000L;
	private final long UNCATEGORIZED_HIT = 3600000L;
	private final long DEFAULT_HIT = 1800000L;
	
	private final int fb_hit = 1;
	private final int fs_hit = 2;
	
    public static void main(String[] args)
    {
        System.out.println("OneOffProcess executed.");
        new TrendCalculator();
    }    
    
    public TrendCalculator(){
    	System.out.println("calculating trend...");
    	calculateTrend();
    }
    
    public void calculateTrend(){
    	long beginTime = System.currentTimeMillis();
    	
    	// use the MongoURI to access MongoDB connection methods.
    	MongoURI uri = new MongoURI(uriString);
    	DB database = null;
    	Mongo mongo = null;
    	DBCollection locations = null;	

    	try {
    		mongo = uri.connect();
    	    database = uri.connectDB();
    	    database.authenticate(uri.getUsername(), uri.getPassword());

		} catch (UnknownHostException uhe) {
			System.out.println("UnknownHostException: " + uhe);
		} catch (MongoException me) {
			System.out.println("MongoException: " + me);
		}
		if(mongo != null){
			char[] pass = new String("XXXXXXXXXXXX").toCharArray(); //removed for public repo
			Morphia morphia = new Morphia();
	    	morphia.map(VenueDto.class).map(FoursquareObj.class).map(FacebookObj.class).map(HitDto.class);
	    	
			datastore = morphia.createDatastore(mongo, "citytrend_prototype", "ctytrend", pass);

			updateExpiredHits();
			fetchFoursquareData();
			fetchFacebookData();
			updateVenueHitScores();

		}
    	
    	long endTime = System.currentTimeMillis();
    	
    	System.out.println("Total trend calculation time: " + ((Double.valueOf(endTime) - Double.valueOf(beginTime))/Double.valueOf(1000)) + " seconds");
    }
    
    private void fetchFacebookData() {
    	ArrayList<String> facebookIds = new ArrayList<String>();
    	ArrayList<FacebookObj> facebookObjsToUpdate = new ArrayList<FacebookObj>();
    	
    	Venues venues = new Venues();
    	
    	FacebookClient facebookClient = new DefaultFacebookClient(oAuthKey);
    	
    	Query<VenueDto> query = datastore.find(VenueDto.class);
		for(VenueDto venueDto: query){
			venues.add(venueDto);
			FacebookObj facebookObj = venueDto.getFacebookObj();
			if(!facebookObj.getFacebookId().equals("null")){
				facebookIds.add(facebookObj.getFacebookId());
				facebookObjsToUpdate.add(facebookObj);
			}
		}
		
		JsonObject fetchObjectsResults = facebookClient.fetchObjects(facebookIds, JsonObject.class);
		JsonMapper jsonMapper = new DefaultJsonMapper();
		
		for(int i = 0; i < facebookIds.size(); i ++){
			Page page = jsonMapper.toJavaObject(fetchObjectsResults.getString(facebookIds.get(i)), Page.class);
			
			for(FacebookObj facebookObj: facebookObjsToUpdate){
				int numOfHitsToCreate = 0;

				//determine what venueId were working with
				if(page.getId().equals(facebookObj.getFacebookId())){
					VenueDto venueDto = venues.findVenue(facebookObj.getVenueId());
					
					// if there have been new checkins create hits
					if(page.getCheckins() > facebookObj.getCurrentCheckins()){
						numOfHitsToCreate = (page.getCheckins()) - (facebookObj.getCurrentCheckins());
						facebookObj.setCurrentCheckins(page.getCheckins());

						System.out.println(venueDto.getVenueName() + " creating " + numOfHitsToCreate + " hits...");
						createHits(venueDto, fb_hit, numOfHitsToCreate);
					}
					venueDto.setFacebookObj(facebookObj);
					datastore.save(venueDto);
				}
			}

		}
	}

	public void fetchFoursquareData(){
		Query<VenueDto> query = datastore.find(VenueDto.class);
		Venues updatedVenues = new Venues();
		
		FoursquareApi foursquareApi = new FoursquareApi(foursquareClientId,foursquareClientSecret, "http://www.citytrend.me");

		for(VenueDto venueDto: query.asList()){

			try {
				Result<CompleteVenue> result = foursquareApi.venue(venueDto.getFoursquareObj().getFoursquareId());

				if (result.getMeta().getCode() == 200) {
					int numOfHitsToCreate = 0;
					FoursquareObj foursquareObj = venueDto.getFoursquareObj();
					
					if(result.getResult().getStats().getCheckinsCount() > foursquareObj.getTotalCheckins()){
						numOfHitsToCreate = (result.getResult().getStats().getCheckinsCount()) - (foursquareObj.getTotalCheckins());
						System.out.println(venueDto.getVenueName() + " creating " + numOfHitsToCreate + " hits...");
						createHits(venueDto, fs_hit, numOfHitsToCreate);
					}
					
					foursquareObj.setTotalCheckins(result.getResult().getStats().getCheckinsCount());
					foursquareObj.setCurrentCheckins(result.getResult().getHereNow().getCount());
					foursquareObj.setUniqueUsers(result.getResult().getStats().getUsersCount());
					venueDto.setFoursquareObj(foursquareObj);					
				}
				updatedVenues.add(venueDto);
				datastore.save(venueDto);

			} catch (FoursquareApiException ex) {
				// do nothing
			}
		}

    }
    
    private void updateExpiredHits(){
    	Query<VenueDto> venueQuery = datastore.find(VenueDto.class);
		Venues venues = new Venues();
		for(VenueDto venueDto: venueQuery){
			venues.add(venueDto);
		}
		
		Query<HitDto> hitQuery = datastore.find(HitDto.class, "expired", false);
		Hits activeHits = new Hits();
		activeHits.addAll(hitQuery.asList());
		
		System.out.println("Hits returned: " + hitQuery.asList().size());
		
		for(HitDto currentHitDto: hitQuery.asList()){
			VenueDto currentVenue = venues.findVenue(currentHitDto.getVenueId());
			
			long currentTime = System.currentTimeMillis();
			long hitExpiration = currentHitDto.getCreateTime() + getVenueLifetime(currentVenue);
			
			if(currentTime >= hitExpiration){
				currentHitDto.setExpired(true);
				datastore.save(currentHitDto);
	    		System.out.println(currentVenue.getVenueName() + ", Hit expired");
			}
		}
    }
   
    
    public void updateVenueHitScores(){
    	Venues updatedVenues = new Venues();
    	
    	Query<VenueDto> venueQuery = datastore.find(VenueDto.class);
		Venues venues = new Venues();
		for(VenueDto venueDto: venueQuery){
			venues.add(venueDto);
		}
		
		Query<HitDto> hitQuery = datastore.find(HitDto.class, "expired", false);
		Hits activeHits = new Hits();
		for(HitDto hitDto: hitQuery){
			activeHits.add(hitDto);
		}

		for(VenueDto venueDto: venues.asList()){
			Hits venuesHits = activeHits.findVenueHits(venueDto.getVenueId());
			
			double venueCurrentTotalHitScore = 0;
			int venueCurrentActiveHits = 0;

			for(HitDto hitDto: venuesHits.asList()){
				venueCurrentTotalHitScore += hitDto.getScore();
				venueCurrentActiveHits ++;
			}
			
			venueDto.setTotalHitScore(venueCurrentTotalHitScore);
			venueDto.setNumCurrentHits(venueCurrentActiveHits);
			updatedVenues.add(venueDto);
		}
		
		Collections.sort(updatedVenues.asList(), Collections.reverseOrder());
		
		int count = 1;
		for(int s = 0; s < updatedVenues.size(); s++){
			VenueDto venueDto = updatedVenues.get(s);
			venueDto.setRank(count);
			System.out.println(count + " " + venueDto.getVenueName() + ", score: " + venueDto.getTotalHitScore());
			datastore.save(venueDto);
			count++;
		}

    }
		
		private long getVenueLifetime(VenueDto venueDto){
			long hitLifetime;
			switch (venueDto.getCategory()) {
			case 1:  hitLifetime = FASTFOOD_HIT;
	                 break;
	        case 2:  hitLifetime = RESTAURANT_HIT;
	          		 break;
	        case 3:  hitLifetime = BAR_HIT;
	        		 break;
	        case 4:  hitLifetime = CLUB_HIT;
	        		 break;
	        case 5:  hitLifetime = ENTERTAINMENT_HIT;
	        		 break;
	        case 6:  hitLifetime = STORE_HIT;
	        		 break;
	        case 7:  hitLifetime = MUSEUM_HIT;
	        		 break;
	        case 8:  hitLifetime = CAFE_HIT;
	        		 break;
	        default: hitLifetime = UNCATEGORIZED_HIT;
	        		 break;
			}
			return hitLifetime;
		}
    
    private void createHits(VenueDto venueDto, int type, int count){
    	for(int i = 0; i< count; i++){
    		HitDto hitDto = new HitDto(venueDto.getVenueId());
    		hitDto.setType(type);
    		datastore.save(hitDto);
    	}
    }
}

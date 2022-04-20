import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import org.bson.Document;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MongoToMongo {
	
    private static String urlcloud = "mongodb://root:teste124@194.210.86.10:27017/?authSource=admin";
    private static String urllocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replicaSet=replicaimdb";
    private static String database="sid2022";
    private static String collectionsensort1 = "sensort1";
    private static String collectionsensort2 = "sensort2";
    private static String collectionsensorh1 = "sensorh1";
    private static String collectionsensorh2 = "sensorh2";
    private static String collectionsensorl1 = "sensorl1";
    private static String collectionsensorl2 = "sensorl2";
    private static int MAXDOCUMENTS = 12;
    
	public void insertCollection(MongoDatabase localMongoDatabase, String collection, MongoDatabase cloudMongoDatabase) {
		MongoCollection<Document> localCollection = localMongoDatabase.getCollection(collection);
		MongoCollection<Document> cloudMongoCollection = cloudMongoDatabase.getCollection(collection);
		List<Document> listat1 = new ArrayList<Document>();
		
		if(localCollection.countDocuments() == 0) {
			localCollection = localMongoDatabase.getCollection(collection);
			for (Document doc : cloudMongoCollection.find().sort(new BasicDBObject("_id",-1)).limit(MAXDOCUMENTS)) {
				if (doc != null && !doc.isEmpty())
					listat1.add(doc);
			}
		}else {
			Document recentDoc = localCollection.find().sort(new BasicDBObject("_id",-1)).first();
			BasicDBObject criteria = new BasicDBObject();
			criteria.append("Data", new BasicDBObject("$gt", recentDoc.getString("Data")));
			
			for (Document doc : cloudMongoCollection.find(criteria)) {
				if (doc != null && !doc.isEmpty())
					listat1.add(doc);
			}
		}
		//System.out.println("A gerar novos dados no Mongo Local: zona " + zona + " Sensor " + sensor);
		if(!listat1.isEmpty())
			localCollection.insertMany(listat1);
	}

    public static void main(String args[]) throws InterruptedException {
        System.out.println("Started...");
        System.out.println("conexao db local");
        //conexao db local
		MongoClient localMongoClient = new MongoClient(new MongoClientURI(urllocal));
	    MongoDatabase localMongoDatabase = localMongoClient.getDatabase(database);
	    System.out.println("conexao db cloud");
        //conexao db cloud
		MongoClient cloudMongoClient = new MongoClient(new MongoClientURI(urlcloud));
	    MongoDatabase cloudMongoDatabase = cloudMongoClient.getDatabase(database);
	    MongoToMongo mongoToMongo = new MongoToMongo();
        while (true){
        	mongoToMongo.insertCollection(localMongoDatabase, collectionsensort1, cloudMongoDatabase);
        	mongoToMongo.insertCollection(localMongoDatabase, collectionsensorh1, cloudMongoDatabase);
        	mongoToMongo.insertCollection(localMongoDatabase, collectionsensorl1, cloudMongoDatabase);
        	mongoToMongo.insertCollection(localMongoDatabase, collectionsensort2, cloudMongoDatabase);
        	mongoToMongo.insertCollection(localMongoDatabase, collectionsensorh2, cloudMongoDatabase);
        	mongoToMongo.insertCollection(localMongoDatabase, collectionsensorl2, cloudMongoDatabase);
            TimeUnit.SECONDS.sleep(5);
        }
    }
}

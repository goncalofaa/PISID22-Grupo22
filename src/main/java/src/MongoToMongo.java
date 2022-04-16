package src;


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

    private static String urlcloud = "mongodb://aluno:aluno@194.210.86.10:27017/?authSource=admin";
    private static String urllocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replicaSet=replicaimdb";

    private static String database="sid2022";

    private static String cloudCollection = "medicoes";

    private static String collectionsensort1 = "sensort1";
    private static String collectionsensort2 = "sensort2";
    private static String collectionsensorh1 = "sensorh1";
    private static String collectionsensorh2 = "sensorh2";
    private static String collectionsensorl1 = "sensorl1";
    private static String collectionsensorl2 = "sensorl2";

    private static int MAXDOCUMENTS = 12;

    public void insertCollection(MongoDatabase localMongoDatabase, String zona, String sensor, String collection, MongoCollection<Document> cloudCollection) {
        MongoCollection<Document> localCollection = localMongoDatabase.getCollection(collection);
        List<Document> listat1 = new ArrayList<Document>();

        BasicDBObject criteria = new BasicDBObject();
        criteria.append("Zona", zona);
        criteria.append("Sensor", sensor);

        if(localCollection.countDocuments() == 0) {
            localMongoDatabase.createCollection(collection);
            localCollection = localMongoDatabase.getCollection(collection);
            for (Document t1 : cloudCollection.find(criteria).sort(new BasicDBObject("_id",-1)).limit(MAXDOCUMENTS)) {
                if (t1 != null && !t1.isEmpty())
                    listat1.add(t1);
            }
        }else {
            Document recentDoc = localCollection.find().sort(new BasicDBObject("_id",-1)).first();
            criteria.append("Data", new BasicDBObject("$gt", recentDoc.getString("Data")));
            //teste
//		    MongoCursor<Document> cursor = cloudCollection.find(criteria).iterator();
//			try {
//			    while (cursor.hasNext()) {
//			        System.out.println(cursor.next().toJson());
//			    }
//			} finally {
//			    cursor.close();
//			}
            //

            for (Document t1 : cloudCollection.find(criteria)) {
                if (t1 != null && !t1.isEmpty())
                    listat1.add(t1);
            }
        }
        //System.out.println("A gerar novos dados no Mongo Local: zona " + zona + " Sensor " + sensor);
        localCollection.insertMany(listat1);
    }

    public static void main(String args[]) throws InterruptedException {
        System.out.println("Started...");
        System.out.println("conexao db local");
        //conexao db local
        MongoClient localMongoClient = new MongoClient(new MongoClientURI(urllocal));
        MongoDatabase localMongoDatabase = localMongoClient.getDatabase(database);
        //MongoCollection<Document> localMongoCollectiont1 = localMongoDatabase.getCollection(collectionsensort1);
        System.out.println("conexao db cloud");
        //conexao db cloud
        MongoClient cloudMongoClient = new MongoClient(new MongoClientURI(urlcloud));
        MongoDatabase cloudMongoDatabase = cloudMongoClient.getDatabase(database);
        MongoCollection<Document> cloudMongoCollection = cloudMongoDatabase.getCollection(cloudCollection);
        MongoToMongo mongoToMongo = new MongoToMongo();
        new ThreadGerarMedicoes("Z1","T1",cloudMongoCollection).start();
        new ThreadGerarMedicoes("Z1","H1",cloudMongoCollection).start();
        new ThreadGerarMedicoes("Z1","L1",cloudMongoCollection).start();
        new ThreadGerarMedicoes("Z2","T2",cloudMongoCollection).start();
        new ThreadGerarMedicoes("Z2","H2",cloudMongoCollection).start();
        new ThreadGerarMedicoes("Z2","L2",cloudMongoCollection).start();
        while (true){
            mongoToMongo.insertCollection(localMongoDatabase,"Z1","T1", collectionsensort1, cloudMongoCollection);
            mongoToMongo.insertCollection(localMongoDatabase,"Z1","H1", collectionsensorh1, cloudMongoCollection);
            mongoToMongo.insertCollection(localMongoDatabase,"Z1","L1", collectionsensorl1, cloudMongoCollection);
            mongoToMongo.insertCollection(localMongoDatabase,"Z2","T2", collectionsensort2, cloudMongoCollection);
            mongoToMongo.insertCollection(localMongoDatabase,"Z2","H2", collectionsensorh2, cloudMongoCollection);
            mongoToMongo.insertCollection(localMongoDatabase,"Z2","L2", collectionsensorl2, cloudMongoCollection);
            TimeUnit.SECONDS.sleep(5);
        }

    }
}

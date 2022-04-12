package src;

import java.util.LinkedList;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

public class MongoToMongoTest {

    // Aqui está o url para aceder aos dados da nuvem
    String urlCloud = "mongodb://aluno:aluno@194.210.86.10:27017/?authSource=admin";
    // Aqui está o url para colocar os dados nas réplicas do meu Mongo local
    String urlLocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replSet=replicaimdb";
    // Aqui está o url para adicionar os dados numa só réplica do Mongo Local
    String urlLocalUnique = "mongodb://localhost:27019";
    // Base de dados da Nuvem
    String cloudDatabase = "sid2021";
    // Base de dados local
    String localDatabase = "medicoes";

    MongoClient cloudMongoClient = new MongoClient(new MongoClientURI(urlCloud));
    MongoDatabase cloudMongoDatabase = cloudMongoClient.getDatabase(cloudDatabase);

    MongoClient localClient = new MongoClient(new MongoClientURI(urlLocal));
    MongoDatabase localMongoDatabase = localClient.getDatabase(localDatabase);

    public void migrate(String sensorId) {

        MongoCollection<Document> cloudCollection = cloudMongoDatabase.getCollection("sensor" + sensorId);
        MongoCollection<Document> localCollection = localMongoDatabase.getCollection(sensorId);

        FindIterable<Document> localList = localCollection.find().sort(Sorts.descending("_id")).limit(1);
        Document last = localList.first();
        System.out.println(last);

        if (last != null) {
            String lastDate = last.getString("Data");
            long count = cloudCollection.countDocuments(Filters.gt("Data", lastDate));
            System.out.println(count);
            if (count > 0) {
                FindIterable<Document> newMeasurements = cloudCollection.find(Filters.gt("Data", lastDate));
                List<Document> list = new LinkedList<Document>();
                for (Document d : newMeasurements) {
                    list.add(d);
                }
                localCollection.insertMany(list);
            }
        } else {
            System.out.println(cloudCollection.countDocuments());
            FindIterable<Document> newMeasurements = cloudCollection.find();
            List<Document> list = new LinkedList<Document>();
            for (Document d : newMeasurements) {
                list.add(d);
            }
            localCollection.insertMany(list);
        }

    }

    public static void main(String[] args) throws InterruptedException {

        MongoToMongoTest con = new MongoToMongoTest();
        while (true) {
            Thread.sleep(5000);
            con.migrate("t1");
            con.migrate("t2");
            con.migrate("h1");
            con.migrate("h2");
            con.migrate("l1");
            con.migrate("l2");
        }
    }
}

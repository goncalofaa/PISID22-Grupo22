import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import org.bson.Document;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class MainGrupo26 {
    
    private static int periodo = 1;
    private static String urlcloud = "mongodb://root:teste124@194.210.86.10:27017/?authSource=admin";
    private static String urllocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replicaSet=medicoespisid";
    private static String database="sid2022";
    private static String collectionsensort1 = "sensort1";
    private static String collectionsensort2 = "sensort2";
    private static String collectionsensorh1 = "sensorh1";
    private static String collectionsensorh2 = "sensorh2";
    private static String collectionsensorl1 = "sensorl1";
    private static String collectionsensorl2 = "sensorl2";
    private static String collectionsmedicoes = "medicoes2022";
    private static int MAXDOCUMENTS = 12;
    private static ThreadSensorGrupo26 h1;
    private static ThreadSensorGrupo26 h2;
    private static ThreadSensorGrupo26 l1;
    private static ThreadSensorGrupo26 l2;
    private static ThreadSensorGrupo26 t1;
    private static ThreadSensorGrupo26 t2;
    private static String message;

    private static String db = "monitorizacao";
    private static String DBuser = "root";
    private static String DBpass = "";
    private static Connection connectionSQL;
    private static HashMap<String, LocalDateTime> datas = new HashMap<>(){{
        put("T1", null);
        put("T2", null);
        put("L1", null);
        put("L2", null);
        put("H1", null);
        put("H2", null);
    }};
    private static HashMap<String, Object > sensoresfuncoes = new HashMap<>(){{
    }};

    public static void insertCollection(MongoDatabase localMongoDatabase, String collection, MongoDatabase cloudMongoDatabase, String collectionCloud, String zona, String sensor) {
        MongoCollection<Document> localCollection = localMongoDatabase.getCollection(collection);
        MongoCollection<Document> cloudMongoCollection = cloudMongoDatabase.getCollection(collectionCloud);
        List<Document> listat1 = new ArrayList<Document>();
        BasicDBObject criteria = new BasicDBObject();
        criteria.append("Zona", zona);
        criteria.append("Sensor", sensor);
        if(localCollection.countDocuments() == 0) {
            localCollection = localMongoDatabase.getCollection(collection);
            for (Document doc : cloudMongoCollection.find(criteria).sort(new BasicDBObject("_id",-1)).limit(MAXDOCUMENTS)) {
                if (doc != null && !doc.isEmpty())
                    listat1.add(doc);
            }
        }else {
            Document recentDoc = localCollection.find().sort(new BasicDBObject("_id",-1)).first();
            //	BasicDBObject criteria = new BasicDBObject();
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

    public static void main(String args[]) throws InterruptedException, SQLException {
        System.out.println("Started...");
        System.out.println("conexao db local");
        //conexao db local
        MongoClient localMongoClient = new MongoClient(new MongoClientURI(urllocal));
        MongoDatabase localMongoDatabase = localMongoClient.getDatabase(database);
        System.out.println("conexao db cloud");
        //conexao db cloud
        MongoClient cloudMongoClient = new MongoClient(new MongoClientURI(urlcloud));
        MongoDatabase cloudMongoDatabase = cloudMongoClient.getDatabase(database);
        MongoToMongo MainGrupo26 = new MongoToMongo();
        MysqlConnection msConnection = new MysqlConnection();
        MysqlProfConnection connectionLocal = new MysqlProfConnection("monitorizacao", "root", "", "localhost");
        MysqlProfConnection connectionNuvem = new MysqlProfConnection("sid2022", "aluno", "aluno", "194.210.86.10");
        PreparedStatement st = connectionNuvem.getConnectionSQL().prepareStatement("SELECT * FROM sensor");
        ResultSet rs = st.executeQuery();
        if(rs.getInt("idzona") == 1 && rs.getString("tipo").equals("H"))  {
            h1 = new ThreadSensorGrupo26(connectionLocal, collectionsensorh1, rs.getInt("limiteinferior"), rs.getInt("limitesuperior"),"H1","1");
            h1.start();
        }
        if(rs.getInt("idzona") == 1 && rs.getString("tipo").equals("L"))  {
             l1 = new ThreadSensorGrupo26(connectionLocal, collectionsensorl1, rs.getInt("limiteinferior"), rs.getInt("limitesuperior"),"L1","1");
            l1.start();
        }
        if(rs.getInt("idzona") == 1 && rs.getString("tipo").equals("T"))  {
            t1 = new ThreadSensorGrupo26(connectionLocal, collectionsensort1, rs.getInt("limiteinferior"), rs.getInt("limitesuperior"),"T1","1");
           t1.start();
        }
        if(rs.getInt("idzona") == 2 && rs.getString("tipo").equals("H"))  {
             h2 = new ThreadSensorGrupo26(connectionLocal, collectionsensorh2, rs.getInt("limiteinferior"), rs.getInt("limitesuperior"),"H2","2");
            h2.start();
        }
        if(rs.getInt("idzona") == 2 && rs.getString("tipo").equals("L"))  {
             l2 = new ThreadSensorGrupo26(connectionLocal, collectionsensorl2, rs.getInt("limiteinferior"), rs.getInt("limitesuperior"),"L2","2");
            l2.start();
        }
        if(rs.getInt("idzona") == 2 && rs.getString("tipo").equals("T"))  {
             t2 = new ThreadSensorGrupo26(connectionLocal, collectionsensort2, rs.getInt("limiteinferior"), rs.getInt("limitesuperior"),"T2","2");
            t2.start();
        }
        sensoresfuncoes = new HashMap<>(){{
            put("T1", t1.setMessage(message));
            put("T2", t2.setMessage(message));
            put("L1", l1.setMessage(message));
            put("L2", l2.setMessage(message));
            put("H1", h1.setMessage(message));
            put("H2", h2.setMessage(message));
        }};
        while (true){
            insertCollection(localMongoDatabase, collectionsensort1, cloudMongoDatabase, collectionsmedicoes, "Z1", "T1");
            insertCollection(localMongoDatabase, collectionsensort1, cloudMongoDatabase, collectionsmedicoes, "Z1", "T1");
            insertCollection(localMongoDatabase, collectionsensorh1, cloudMongoDatabase, collectionsmedicoes, "Z1", "H1");
            insertCollection(localMongoDatabase, collectionsensorl1, cloudMongoDatabase, collectionsmedicoes, "Z1", "L1");
            insertCollection(localMongoDatabase, collectionsensort2, cloudMongoDatabase, collectionsmedicoes, "Z2", "T2");
            insertCollection(localMongoDatabase, collectionsensorh2, cloudMongoDatabase, collectionsmedicoes, "Z2", "H2");
            insertCollection(localMongoDatabase, collectionsensorl2, cloudMongoDatabase, collectionsmedicoes, "Z2", "L2");
            publishDocument(collectionsensort1,"T1",localMongoDatabase);
            publishDocument(collectionsensort2,"T2",localMongoDatabase);
            publishDocument(collectionsensorh1,"H1",localMongoDatabase);
            publishDocument(collectionsensorh2,"H2",localMongoDatabase);
            publishDocument(collectionsensorl1,"L1",localMongoDatabase);
            publishDocument(collectionsensorl2,"L2",localMongoDatabase);
            TimeUnit.SECONDS.sleep(periodo);
        }
    }

    public static void publishDocument(String collection, String sensor,MongoDatabase localMongoDatabase) {

        MongoCollection<Document> localCollection = localMongoDatabase.getCollection(collection);
        Document recentDoc = localCollection.find().sort(new BasicDBObject("_id",-1)).first();

        LocalDateTime datarec1 = datas.get(sensor);
        LocalDateTime dataRecenteMongo = null;
        String dataRecMongo = recentDoc.getString("Data");
        String rawMsg = "Zona:" + recentDoc.getString("Zona") + ";" + "Sensor:" +
                recentDoc.getString("Sensor") + ";" + "Data:" + recentDoc.getString("Data") + ";" +
                "Medicao:" + recentDoc.getString("Medicao");
        message=rawMsg;
        if(dataRecMongo != null && !dataRecMongo.isEmpty()) {
            dataRecMongo=dataRecMongo.replace("T", " ");
            dataRecMongo=dataRecMongo.replace("Z", "");
            dataRecMongo=dataRecMongo.replace("-", " ");
            dataRecMongo=dataRecMongo.replace(":", " ");
            String[] datSplit = dataRecMongo.split(" ");
            dataRecenteMongo = LocalDateTime.of(Integer.parseInt(datSplit[0]), Integer.parseInt(datSplit[1]), Integer.parseInt(datSplit[2]),
                    Integer.parseInt(datSplit[3]), Integer.parseInt(datSplit[4]), Integer.parseInt(datSplit[5]));
        }
        if(datarec1 != null && dataRecenteMongo != null && dataRecenteMongo.isAfter(datarec1)) {
            System.out.println(datarec1.toString());
            System.out.println(dataRecenteMongo.toString());
            sensoresfuncoes.get(sensor);
        }else if(datarec1 == null && dataRecenteMongo != null) {
            sensoresfuncoes.get(sensor);
        }
        datas.put(sensor, dataRecenteMongo);
    }

}

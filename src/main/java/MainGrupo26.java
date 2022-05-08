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
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class MainGrupo26 {

    private static String urlcloud = "mongodb://root:teste124@194.210.86.10:27017/?authSource=admin";
    private static String urllocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replicaSet=replicaImdb";
    private static String database="sid2022";
    private static String collectionsensort1 = "sensort1";
    private static String collectionsensort2 = "sensort2";
    private static String collectionsensorh1 = "sensorh1";
    private static String collectionsensorh2 = "sensorh2";
    private static String collectionsensorl1 = "sensorl1";
    private static String collectionsensorl2 = "sensorl2";
    private static String collectiont1 = "sensort1";
    private static String collectiont2 = "sensort2";
    private static String collectionh1 = "sensorh1";
    private static String collectionh2 = "sensorh2";
    private static String collectionl1 = "sensorl1";
    private static String collectionl2 = "sensorl2";
    private LocalDateTime datat1 = LocalDateTime.now();
    private LocalDateTime datat2 = LocalDateTime.now();
    private LocalDateTime datal1 = LocalDateTime.now();
    private LocalDateTime datal2 = LocalDateTime.now();
    private LocalDateTime datah1 = LocalDateTime.now();
    private LocalDateTime datah2 = LocalDateTime.now();
    private static BlockingQueue<String> messageListt1 = new LinkedBlockingDeque<>();
    private static BlockingQueue<String> messageListt2 = new LinkedBlockingDeque<>();
    private static BlockingQueue<String> messageListl1 = new LinkedBlockingDeque<>();
    private static BlockingQueue<String> messageListl2 = new LinkedBlockingDeque<>();
    private static BlockingQueue<String> messageListh1 = new LinkedBlockingDeque<>();
    private static BlockingQueue<String> messageListh2 = new LinkedBlockingDeque<>();
    private static String messaget1;
    private static String messaget2;
    private static String messagel1;
    private static String messagel2;
    private static String messageh1;
    private static String messageh2;


    private static HashMap<String, LocalDateTime> datas = new HashMap<>(){{
        put("T1", null);
        put("T2", null);
        put("L1", null);
        put("L2", null);
        put("H1", null);
        put("H2", null);
    }};

    private static HashMap<String, BlockingQueue<String>> messsalists = new HashMap<>(){{
        put("T1", new LinkedBlockingDeque<>());
        put("T2", new LinkedBlockingDeque<>());
        put("L1", new LinkedBlockingDeque<>());
        put("L2", new LinkedBlockingDeque<>());
        put("H1", new LinkedBlockingDeque<>());
        put("H2", new LinkedBlockingDeque<>());
    }};

    private static final int TEMPOENVIO = 5000;
    private static String collectionsensorCloud = "medicoes2022";
    private static int MAXDOCUMENTS = 12;

    public static void main(String[] args) throws InterruptedException, SQLException {
        //inicar Threads
        MysqlProfConnection connectionLocal = new MysqlProfConnection("monitorizacao", "root", "", "localhost");
        MysqlProfConnection connectionNuvem = new MysqlProfConnection("sid2022", "aluno", "aluno", "194.210.86.10");
        PreparedStatement st = connectionNuvem.getConnectionSQL().prepareStatement("SELECT * FROM sensor");
        ResultSet rs = st.executeQuery();
        CyclicBarrier newBarrier = new CyclicBarrier(6);
        while(rs.next()) {
            if(rs.getInt("idzona") == 1 && rs.getString("tipo").equals("H"))  {
                new ThreadSensorGrupo26(newBarrier,connectionLocal, collectionsensorh1, rs.getInt("limiteinferior"), rs.getInt("limitesuperior"),"H1","1",messsalists.get("H1")).start();
            }
            if(rs.getInt("idzona") == 1 && rs.getString("tipo").equals("L"))  {
                new ThreadSensorGrupo26(newBarrier,connectionLocal, collectionsensorl1, rs.getInt("limiteinferior"), rs.getInt("limitesuperior"),"L1","1",messsalists.get("L1")).start();
            }
            if(rs.getInt("idzona") == 1 && rs.getString("tipo").equals("T"))  {
                new ThreadSensorGrupo26(newBarrier,connectionLocal, collectionsensort1, rs.getInt("limiteinferior"), rs.getInt("limitesuperior"),"T1","1",messsalists.get("T1")).start();
            }
            if(rs.getInt("idzona") == 2 && rs.getString("tipo").equals("H"))  {
                new ThreadSensorGrupo26(newBarrier,connectionLocal, collectionsensorh2, rs.getInt("limiteinferior"), rs.getInt("limitesuperior"),"H2","2",messsalists.get("H2")).start();
            }
            if(rs.getInt("idzona") == 2 && rs.getString("tipo").equals("L"))  {
                new ThreadSensorGrupo26(newBarrier,connectionLocal, collectionsensorl2, rs.getInt("limiteinferior"), rs.getInt("limitesuperior"),"L2","2",messsalists.get("L2")).start();
            }
            if(rs.getInt("idzona") == 2 && rs.getString("tipo").equals("T"))  {
                new ThreadSensorGrupo26(newBarrier,connectionLocal, collectionsensort2, rs.getInt("limiteinferior"), rs.getInt("limitesuperior"),"T2","2",messsalists.get("T2")).start();
            }
        }
        while(true) {
            mongotoMongo();
            mongotoMysql();
        }
    }

    public static void mongotoMongo(){
        //System.out.println("Mongo To Mongo");
        MongoClient localMongoClient = new MongoClient(new MongoClientURI(urllocal));
        MongoDatabase localMongoDatabase = localMongoClient.getDatabase(database);
        MongoClient cloudMongoClient = new MongoClient(new MongoClientURI(urlcloud));
        MongoDatabase cloudMongoDatabase = cloudMongoClient.getDatabase(database);
        insertCollection(localMongoDatabase, collectionsensort1, cloudMongoDatabase, collectionsensorCloud, "Z1","T1");
        insertCollection(localMongoDatabase, collectionsensorh1, cloudMongoDatabase, collectionsensorCloud, "Z1","H1");
        insertCollection(localMongoDatabase, collectionsensorl1, cloudMongoDatabase, collectionsensorCloud, "Z1","L1");
        insertCollection(localMongoDatabase, collectionsensort2, cloudMongoDatabase, collectionsensorCloud, "Z2","T2");
        insertCollection(localMongoDatabase, collectionsensorh2, cloudMongoDatabase, collectionsensorCloud, "Z2","H2");
        insertCollection(localMongoDatabase, collectionsensorl2, cloudMongoDatabase, collectionsensorCloud, "Z2","L2");
    }

    public static void insertCollection(MongoDatabase localMongoDatabase, String collection, MongoDatabase cloudMongoDatabase, String collectionsensorCloud, String zona, String sensor) {
        MongoCollection<Document> localCollection = localMongoDatabase.getCollection(collection);
        MongoCollection<Document> cloudMongoCollection = cloudMongoDatabase.getCollection(collectionsensorCloud);
        List<Document> listat1 = new ArrayList<Document>();
        BasicDBObject criteria = new BasicDBObject();
        criteria.append("Zona", zona);
        criteria.append("Sensor", sensor);
        if(localCollection.countDocuments() == 0) {
            //System.out.println("If");
            localCollection = localMongoDatabase.getCollection(collection);
            for (Document doc : cloudMongoCollection.find(criteria).sort(new BasicDBObject("_id",-1)).limit(MAXDOCUMENTS)) {
                if (doc != null && !doc.isEmpty())
                    listat1.add(doc);
            }
        }else {
            //System.out.println("Else");
            Document recentDoc = localCollection.find().sort(new BasicDBObject("_id",-1)).first();
            criteria.append("Data", new BasicDBObject("$gt", recentDoc.getString("Data")));

            for (Document doc : cloudMongoCollection.find(criteria)) {
                if (doc != null && !doc.isEmpty())
                    listat1.add(doc);
            }
        }
        if(!listat1.isEmpty())
            localCollection.insertMany(listat1);
    }

    private static void mongotoMysql() throws InterruptedException {
        //System.out.println("Mongo To Mysql");
        MongoClient localMongoClient = new MongoClient(new MongoClientURI(urllocal));
        MongoDatabase localMongoDatabase = localMongoClient.getDatabase(database);
        publishDocument(collectiont1,"T1",localMongoDatabase);
        publishDocument(collectiont2,"T2", localMongoDatabase);
        publishDocument(collectionh1,"H1", localMongoDatabase);
        publishDocument(collectionh2,"H2", localMongoDatabase);
        publishDocument(collectionl1,"L1", localMongoDatabase);
        publishDocument(collectionl2,"L2", localMongoDatabase);
        Thread.sleep(TEMPOENVIO);
    }

    public static void publishDocument(String collection, String sensor, MongoDatabase localMongoDatabase) {

        MongoCollection<Document> localCollection = localMongoDatabase.getCollection(collection);
        Document recentDoc = localCollection.find().sort(new BasicDBObject("_id",-1)).first();

        LocalDateTime datarec1 = datas.get(sensor);
        LocalDateTime dataRecenteMongo = null;
        String dataRecMongo = recentDoc.getString("Data");
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
            //System.out.println(datarec1.toString());
            //System.out.println(dataRecenteMongo.toString());
            publicarMensagem(recentDoc, collection, sensor);
        }else if(datarec1 == null && dataRecenteMongo != null) {
            publicarMensagem(recentDoc, collection, sensor);
        }
        datas.put(sensor, dataRecenteMongo);
    }

    private static void publicarMensagem(Document recentDoc, String collection, String sensor) {
        String rawMsg = "Zona:" + recentDoc.getString("Zona") + ";" + "Sensor:" +
                recentDoc.getString("Sensor") + ";" + "Data:" + recentDoc.getString("Data") + ";" +
                "Medicao:" + recentDoc.getString("Medicao");
        SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
        Date date = new Date(System.currentTimeMillis());
        //System.out.println(formatter.format(date));
        System.out.println(rawMsg + " Data Inicio Processamento: " + formatter.format(date));
        try {
            messsalists.get(sensor).put(rawMsg);
        }catch (InterruptedException e){
            e.printStackTrace();
        }


    }


}

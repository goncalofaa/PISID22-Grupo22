import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.List;
import java.util.concurrent.TimeUnit;

//A ver: casos em que a data não é válida
//       se a data é maior
//       primeirso docs puxados têm datas inválidas
//       meter em formatos iguais

public class MongoToMongo {

    //private static String database = "mongodb://java:java@localhost:27019;
    private static String urlcloud = "mongodb://aluno:aluno@194.210.86.10:27017/?authSource=admin";
    private static String urllocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replicaSet=medicoespisid";

    private static String databasecloud="sid2021";
    private static String databaselocal = "medicoes";
    //locais
    private static String collectiont1 = "t1";
    private static String collectiont2 = "t2";
    private static String collectionh1 = "h1";
    private static String collectionh2 = "h2";
    private static String collectionl1 = "l1";
    private static String collectionl2 = "l2";
    //cloud
    private static String collectionsensort1 = "sensort1";
    private static String collectionsensort2 = "sensort2";
    private static String collectionsensorh1 = "sensorh1";
    private static String collectionsensorh2 = "sensorh2";
    private static String collectionsensorl1 = "sensorl1";
    private static String collectionsensorl2 = "sensorl2";
    //coleções locais
    private static MongoCollection<Document> localcollectiont1;
    private static MongoCollection<Document> localcollectiont2;
    private static MongoCollection<Document> localcollectionh1;
    private static MongoCollection<Document> localcollectionh2;
    private static MongoCollection<Document> localcollectionl1;
    private static MongoCollection<Document> localcollectionl2;
    //coleções cloud
    private static MongoCollection<Document> cloudcollectiont1;
    private static MongoCollection<Document> cloudcollectiont2;
    private static MongoCollection<Document> cloudcollectionh1;
    private static MongoCollection<Document> cloudcollectionh2;
    private static MongoCollection<Document> cloudcollectionl1;
    private static MongoCollection<Document> cloudcollectionl2;
    //clients
    private static MongoClient localMongoClient;
    private static MongoClient cloudMongoClient;
    //dbs
    private static MongoDatabase localMongoDatabase;
    private static MongoDatabase cloudMongoDatabase;
    //armazenar datas
    private static String datat1;
    private static String datat2;
    private static String datal1;
    private static String datal2;
    private static String datah1;
    private static String datah2;
    //auxiliares de verificação de vazios iniciais
    private static Boolean t1vazia=false;
    private static Boolean t2vazia=false;
    private static Boolean h1vazia=false;
    private static Boolean h2vazia=false;
    private static Boolean l1vazia=false;
    private static Boolean l2vazia=false;

    public static void main(String args[]) throws InterruptedException {
        System.out.println("Started...");
        localMongoClient = new MongoClient(new MongoClientURI(urllocal));
        cloudMongoClient = new MongoClient(new MongoClientURI(urlcloud));

        checkWichCollectionsAreEmpty();

        getOneDocOnEmptyCollectionsAndUpdateDates();

        while (true){
            migrateNewDocuments();
            System.out.println(System.currentTimeMillis());
            TimeUnit.SECONDS.sleep(1);
        }

    }

    //Retorna True se uma das coleções estiver vazia
    private static void checkWichCollectionsAreEmpty() {
        //Atualiza as inicializa as variáveis das coleções locais
        localMongoDatabase = localMongoClient.getDatabase(databaselocal);
        localcollectiont1 = localMongoDatabase.getCollection(collectiont1);
        localcollectionh1 = localMongoDatabase.getCollection(collectionh1);
        localcollectionl1 = localMongoDatabase.getCollection(collectionl1);
        localcollectiont2 = localMongoDatabase.getCollection(collectiont2);
        localcollectionh2 = localMongoDatabase.getCollection(collectionh2);
        localcollectionl2 = localMongoDatabase.getCollection(collectionl2);
        //verifica cada coleção, se estiver vazia muda a varíavel boleana que indica se a coleção do sensor está vazia para True
        if (localcollectiont1.countDocuments()==0) {t1vazia=true;}
        if (localcollectiont2.countDocuments()==0) {t2vazia=true;}
        if (localcollectionl1.countDocuments()==0) {h1vazia=true;}
        if (localcollectionl2.countDocuments()==0) {h2vazia=true;}
        if (localcollectionh1.countDocuments()==0) {l1vazia=true;}
        if (localcollectionh2.countDocuments()==0) {l2vazia=true;}
        return;
    }

    //Se a coleção local está vazia, vai buscar o documento mais recente da respectiva coleção na cloud
    //Se não, apenas atualiza a respctiva variável data
    private static void getOneDocOnEmptyCollectionsAndUpdateDates(){
        if (t1vazia) {
            cloudMongoDatabase = cloudMongoClient.getDatabase(databasecloud);
            cloudcollectiont1 = cloudMongoDatabase.getCollection(collectionsensort1);
            Document t1aux = cloudcollectiont1.find().sort(new BasicDBObject("_id", -1)).first();
            localcollectiont1.insertOne(t1aux);
            datat1 = (String) t1aux.get("Data");
            t1vazia=false;
        }else {
            datat1=(String) localcollectiont1.find().sort(new BasicDBObject("_id", -1)).first().get("Data");
        }
        System.out.println(datat1);

        if (t2vazia) {
            cloudMongoDatabase = cloudMongoClient.getDatabase(databasecloud);
            cloudcollectiont2 = cloudMongoDatabase.getCollection(collectionsensort2);
            Document t2aux = cloudcollectiont2.find().sort(new BasicDBObject("_id", -1)).first();
            localcollectiont2.insertOne(t2aux);
            datat2 = (String) t2aux.get("Data");
            t2vazia=false;
        }else {
            datat2=(String) localcollectiont2.find().sort(new BasicDBObject("_id", -1)).first().get("Data");
        }
        System.out.println(datat2);

        if (h1vazia) {
            cloudMongoDatabase = cloudMongoClient.getDatabase(databasecloud);
            cloudcollectionh1 = cloudMongoDatabase.getCollection(collectionsensorh1);
            Document h1aux = cloudcollectionh1.find().sort(new BasicDBObject("_id", -1)).first();
            localcollectionh1.insertOne(h1aux);
            datah1 = (String) h1aux.get("Data");
            h1vazia=false;
        }else {
            datah1=(String) localcollectionh1.find().sort(new BasicDBObject("_id", -1)).first().get("Data");
        }
        System.out.println(datah1);

        if (h2vazia) {
            cloudMongoDatabase = cloudMongoClient.getDatabase(databasecloud);
            cloudcollectionh2 = cloudMongoDatabase.getCollection(collectionsensorh2);
            Document h2aux = cloudcollectionh2.find().sort(new BasicDBObject("_id", -1)).first();
            localcollectionh2.insertOne(h2aux);
            datah2 = (String) h2aux.get("Data");
            h2vazia=false;
        }else {
            datah2=(String) localcollectionh2.find().sort(new BasicDBObject("_id", -1)).first().get("Data");
        }
        System.out.println(datah2);

        if (l1vazia) {
            cloudMongoDatabase = cloudMongoClient.getDatabase(databasecloud);
            cloudcollectionl1 = cloudMongoDatabase.getCollection(collectionsensorl1);
            Document l1aux = cloudcollectionl1.find().sort(new BasicDBObject("_id", -1)).first();
            localcollectionl1.insertOne(l1aux);
            datal1 = (String) l1aux.get("Data");
            l1vazia=false;
        }else {
            datal1=(String) localcollectionl1.find().sort(new BasicDBObject("_id", -1)).first().get("Data");
        }
        System.out.println(datal1);

        if (l2vazia) {
            cloudMongoDatabase = cloudMongoClient.getDatabase(databasecloud);
            cloudcollectionl2 = cloudMongoDatabase.getCollection(collectionsensorl2);
            Document l2aux = cloudcollectionl2.find().sort(new BasicDBObject("_id", -1)).first();
            localcollectionl2.insertOne(l2aux);
            datal2 = (String) l2aux.get("Data");
            l2vazia=false;
        }else {
            datal2=(String) localcollectionl2.find().sort(new BasicDBObject("_id", -1)).first().get("Data");
        }
        System.out.println(datal2);
    }

    private static void migrateNewDocuments() {
        //atualiza os valores da cloud
        cloudMongoDatabase = cloudMongoClient.getDatabase(databasecloud);

        //verifica se o documento mais recente na coleção da cloud é mais recente que o da coleção local, se sim insere e atualiza a variável data
        cloudcollectiont1 = cloudMongoDatabase.getCollection(collectionsensort1);
        Document clouddocumentt1 = cloudcollectiont1.find().sort(new BasicDBObject("_id", -1)).first();
        if ((String) clouddocumentt1.get("Data") != datat1){
            if (isBiggerThanSensorT((String) clouddocumentt1.get("Data"),datat1)){
                datat1=(String) clouddocumentt1.get("Data");
                localcollectionl2.insertOne(clouddocumentt1);
                System.out.println("Atualizei t1:"+datat1);
            }
            else{
                //Dá erro no catch armazenar em txt ou usar com mais um segundo que a anterior
            }
        }
        System.out.println("t1 não mudou");

        cloudcollectiont2 = cloudMongoDatabase.getCollection(collectionsensort2);
        Document clouddocumentt2 = cloudcollectiont2.find().sort(new BasicDBObject("_id", -1)).first();
        if ((String) clouddocumentt2.get("Data") != datat2){
            if (isBiggerThanSensorT((String) clouddocumentt2.get("Data"),datat2)){
                datat2=(String) clouddocumentt2.get("Data");
                localcollectionl2.insertOne(clouddocumentt2);
                System.out.println("Atualizei t2:"+datat2);
            }
            else{
                //Dá erro no catch armazenar em txt ou usar com mais um segundo que a anterior
            }
        }
        System.out.println("t2 não mudou");
        
        cloudcollectionh1 = cloudMongoDatabase.getCollection(collectionsensorh1);
        Document clouddocumenth1 = cloudcollectionh1.find().sort(new BasicDBObject("_id", -1)).first();
        if ((String) clouddocumenth1.get("Data") != datah1){
            if (isBiggerThanSensorNonT((String) clouddocumenth1.get("Data"),datah1)){
                datah1=(String) clouddocumenth1.get("Data");
                localcollectionl2.insertOne(clouddocumenth1);
                System.out.println("Atualizei h1:"+datah1);
            }
            else{
                //Dá erro no catch armazenar em txt ou usar com mais um segundo que a anterior
            }
        }
        System.out.println("h1 não mudou");

        cloudcollectionh2 = cloudMongoDatabase.getCollection(collectionsensorh2);
        Document clouddocumenth2 = cloudcollectionh2.find().sort(new BasicDBObject("_id", -1)).first();
        if ((String) clouddocumenth2.get("Data") != datah2){
            if (isBiggerThanSensorNonT((String) clouddocumenth2.get("Data"),datah2)){
                datah2=(String) clouddocumenth2.get("Data");
                localcollectionl2.insertOne(clouddocumenth2);
                System.out.println("Atualizei h2:"+datah2);
            }
            else{
                //Dá erro no catch armazenar em txt ou usar com mais um segundo que a anterior
            }
        }
        System.out.println("h2 não mudou");

        cloudcollectionl1 = cloudMongoDatabase.getCollection(collectionsensorl1);
        Document clouddocumentl1 = cloudcollectionl1.find().sort(new BasicDBObject("_id", -1)).first();
        if ((String) clouddocumentl1.get("Data") != datal1){
            if (isBiggerThanSensorNonT((String) clouddocumentl1.get("Data"),datal1)){
                datal1=(String) clouddocumentl1.get("Data");
                localcollectionl2.insertOne(clouddocumentl1);
                System.out.println("Atualizei l1:"+datal1);
            }
            else{
                //Dá erro no catch armazenar em txt ou usar com mais um segundo que a anterior
            }
        }
        System.out.println("l1 não mudou");

        cloudcollectionl2 = cloudMongoDatabase.getCollection(collectionsensorl2);
        Document clouddocumentl2 = cloudcollectionl2.find().sort(new BasicDBObject("_id", -1)).first();
        if ((String) clouddocumentl2.get("Data") != datal2){
            if (isBiggerThanSensorNonT((String) clouddocumentl2.get("Data"),datal2)){
                datal2=(String) clouddocumentl2.get("Data");
                localcollectionl2.insertOne(clouddocumentl2);
                System.out.println("Atualizei l2:"+datal2);
            }
            else{
                //Dá erro no catch armazenar em txt ou usar com mais um segundo que a anterior
            }
        }
        System.out.println("l2 não mudou");

    }

    //comparador de data, dá erro se a data do doc da cloud é mais recente que o local Formato Sensores T
    private static boolean isBiggerThanSensorT(String datedoccloud, String datedoclocal){
        try {
            List datacloud= List.of(datedoccloud.split("T")[0].split("-"));
            List datalocal= List.of(datedoccloud.split("T")[0].split("-"));
            if(Integer.parseInt((String) datacloud.get(0))>Integer.parseInt((String) datalocal.get(0))){
                return true;
            }
            if(Integer.parseInt((String) datacloud.get(1))>Integer.parseInt((String) datalocal.get(1))){
                return true;
            }
            if(Integer.parseInt((String) datacloud.get(2))>Integer.parseInt((String) datalocal.get(2))){
                return true;
            }
            datacloud= List.of(datedoccloud.split("T")[1].split(":"));
            datalocal= List.of(datedoccloud.split("T")[1].split(":"));
            if(Integer.parseInt((String) datacloud.get(0))>Integer.parseInt((String) datalocal.get(0))){
                return true;
            }
            if(Integer.parseInt((String) datacloud.get(1))>Integer.parseInt((String) datalocal.get(1))){
                return true;
            }
            if(Integer.parseInt((String) ((String) datacloud.get(2)).replace("Z",""))>Integer.parseInt((String) ((String) datalocal.get(2)).replace("Z",""))){
                return true;
            }
            return false;
        }catch (Exception e){
            System.out.println("Data No Formato Errado:"+datedoccloud);
            //logs txt?
            return false;
        }
    }

    //comparador de data, dá erro se a data do doc da cloud é mais recente que o local Formato Sensores Não T
    private static boolean isBiggerThanSensorNonT(String datedoccloud, String datedoclocal){
        try {
            List datacloud= List.of(datedoccloud.split(" at ")[0].split("-"));
            List datalocal= List.of(datedoccloud.split(" at ")[0].split("-"));
            if(Integer.parseInt((String) datacloud.get(0))>Integer.parseInt((String) datalocal.get(0))){
                return true;
            }
            if(Integer.parseInt((String) datacloud.get(1))>Integer.parseInt((String) datalocal.get(1))){
                return true;
            }
            if(Integer.parseInt((String) datacloud.get(2))>Integer.parseInt((String) datalocal.get(2))){
                return true;
            }
            datacloud= List.of(datedoccloud.split(" at ")[1].split(":"));
            datalocal= List.of(datedoccloud.split(" at ")[1].split(":"));
            if(Integer.parseInt((String) datacloud.get(0))>Integer.parseInt((String) datalocal.get(0))){
                return true;
            }
            if(Integer.parseInt((String) datacloud.get(1))>Integer.parseInt((String) datalocal.get(1))){
                return true;
            }
            if(Integer.parseInt((String) ((String) datacloud.get(2)).replace(" GMT",""))>Integer.parseInt((String) ((String) datalocal.get(2)).replace(" GMT",""))){
                return true;
            }
            return false;
        }catch (Exception e){
            System.out.println("Data No Formato Errado:"+datedoccloud);
            //logs txt?
            return false;
        }
    }

}

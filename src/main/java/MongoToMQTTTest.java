import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.BsonDocument;
import org.eclipse.paho.client.mqttv3.*;


import org.bson.Document;
import java.util.UUID;

public class MongoToMQTTTest {

    // Local dadtabase
    private static String databaselocal = "medicoes";

    //CloudServer
    private static String clouServer = "tcp://broker.mqtt-dashboard.com:1883";

    // Url to access to local replics
    String urlLocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replSet=replicaimdb";

    //Topics to subscribe in Broker
    private String topicot1 = "G22T1";
    private String topicot2 = "G22T2";
    private String topicoh1 = "G22H1";
    private String topicoh2 = "G22H2";
    private String topicol1 = "G22L1";
    private String topicol2 = "G22L2";

    //Strings to local Collections
    private static String collectiont1 = "t1";
    private static String collectiont2 = "t2";
    private static String collectionh1 = "h1";
    private static String collectionh2 = "h2";
    private static String collectionl1 = "l1";
    private static String collectionl2 = "l2";

    //Striengs to the last Date
    private static String lastLocalDatet1;
    private static String lastLocalDatet2;
    private static String lastLocalDateh1;
    private static String lastLocalDateh2;
    private static String lastLocalDatel1;
    private static String lastLocalDatel2;


    //Validate if collections are empty or not
    private static Boolean t1vazia = false;
    private static Boolean t2vazia = false;
    private static Boolean h1vazia = false;
    private static Boolean h2vazia = false;
    private static Boolean l1vazia = false;
    private static Boolean l2vazia = false;

    //Local Collections
    private static MongoCollection<Document> localcollectiont1;
    private static MongoCollection<Document> localcollectiont2;
    private static MongoCollection<Document> localcollectionh1;
    private static MongoCollection<Document> localcollectionh2;
    private static MongoCollection<Document> localcollectionl1;
    private static MongoCollection<Document> localcollectionl2;

    //Local String of the last Document
    private String stringDocumentt1;
    private String stringDocumentt2;
    private String stringDocumentl1;
    private String stringDocumentl2;
    private String stringDocumenth1;
    private String stringDocumenth2;

    //String of cloud dates
    private static String cloudLastDatet1;
    private static String cloudLastDatet2;
    private static String cloudLastDatel1;
    private static String cloudLastDatel2;
    private static String cloudLastDateh1;
    private static String cloudLastDateh2;

    //Local Client
    private static MongoClient localMongoClient;

    MongoClient localClient = new MongoClient(new MongoClientURI(urlLocal));
    MongoDatabase localMongoDatabase = localClient.getDatabase(databaselocal);

    private IMqttClient mqttClient;

    public void connectToMQTT() throws MqttException {

        String clientId = UUID.randomUUID().toString();
        mqttClient = new MqttClient(clouServer, clientId);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);
        mqttClient.connect(options);
    }

    public void migrate() throws MqttPersistenceException, MqttException, InterruptedException {

        // Tratamento da coleção t1 local
        localcollectiont1 = localMongoDatabase.getCollection(collectiont1);

        Document dt1 = localcollectiont1.find().sort(new BasicDBObject("_id", -1)).first();

        if (collectiont1.equals("t1")) {
            Thread.sleep(5000);

            String zonat1 = dt1.getString("Zona");
            String sensort1 = dt1.getString("Sensor");
            String datat1 = dt1.getString("Data").replace("T", " ").replace("Z", " ").replace("-", "").replace(":", "");
            String medicaot1 = dt1.getString("Medicao");
            stringDocumentt1 = zonat1 + "|" + sensort1 + "|" + datat1 + "|" + medicaot1;
            byte[] arrayBytet1 = stringDocumentt1.getBytes();


            MqttMessage mqttMessaget1 = new MqttMessage(arrayBytet1);
            mqttMessaget1.setQos(0);
            mqttMessaget1.setRetained(false);
            mqttClient.publish(topicot1, mqttMessaget1);
            System.out.println(mqttMessaget1.toString());

        } else {
            return;
        }


        // Tratamento da coleção t2 local
        localcollectiont2 = localMongoDatabase.getCollection(collectiont2);

        Document dt2 = localcollectiont2.find().sort(new BasicDBObject("_id", -1)).first();

            if (collectiont2.equals("t2")) {
                Thread.sleep(5000);


                    String zonat2 = dt2.getString("Zona");
                    String sensort2 = dt2.getString("Sensor");
                    String datat2 = dt2.getString("Data").replace("T", " ").replace("Z"," ").replace("-","").replace(":", "");
                    String medicaot2 = dt2.getString("Medicao");
                    stringDocumentt2 = zonat2 + "|" + sensort2 + "|" + datat2 + "|" + medicaot2;

                    byte[] arrayBytet2 = stringDocumentt2.getBytes();

                    MqttMessage mqttMessaget2 = new MqttMessage(arrayBytet2);
                    mqttMessaget2.setQos(0);
                    mqttMessaget2.setRetained(false);
                    mqttClient.publish(topicot2, mqttMessaget2);
                    System.out.println(mqttMessaget2.toString());


            } else {
                return;
            }



        // Tratamento da coleção h1 local
        localcollectionh1 = localMongoDatabase.getCollection(collectionh1);

        Document dh1 = localcollectionh1.find().sort(new BasicDBObject("_id", -1)).first();

            if (collectionh1.equals("h1")) {
                Thread.sleep(5000);

                String zonah1 = dh1.getString("Zona");
                String sensorh1 = dh1.getString("Sensor");
                String datah1 = dh1.getString("Data").replace("T", " ").replace("Z"," ").replace("-","").replace(":", "");
                String medicaoh1 = dh1.getString("Medicao");
                stringDocumenth1 = zonah1 + "|" + sensorh1 + "|" + datah1 + "|" + medicaoh1;

                    byte[] arrayByteh1 = stringDocumenth1.getBytes();

                    MqttMessage mqttMessageh1 = new MqttMessage(arrayByteh1);
                    mqttMessageh1.setQos(0);
                    mqttMessageh1.setRetained(false);
                    mqttClient.publish(topicoh1, mqttMessageh1);
                    System.out.println(mqttMessageh1.toString());

            } else {
                return;
            }



        // Tratamento da coleção h2 local
        localcollectionh2 = localMongoDatabase.getCollection(collectionh2);

        Document dh2 = localcollectionh2.find().sort(new BasicDBObject("_id", -1)).first();

            if (collectionh2.equals("h2")) {
                Thread.sleep(5000);

                    String zonah2 = dh2.getString("Zona");
                    String sensorh2 =  dh2.getString("Sensor");
                    String datah2 = dh2.getString("Data").replace("T", " ").replace("Z", " ");
                    String medicaoh2 = dh2.getString("Medicao");
                    stringDocumenth2 = zonah2 + "|" + sensorh2 + "|" + datah2 + "|" + medicaoh2;

                    byte[] arrayByteh2 = stringDocumenth2.getBytes();

                    MqttMessage mqttMessageh2 = new MqttMessage(arrayByteh2);
                    mqttMessageh2.setQos(0);
                    mqttMessageh2.setRetained(false);
                    mqttClient.publish(topicoh2, mqttMessageh2);
                    System.out.println(mqttMessageh2.toString());

            } else {
                return;
            }



        // Tratamento da coleção l1 local
        localcollectionl1 = localMongoDatabase.getCollection(collectionl1);

        Document dl1 = localcollectionl1.find().sort(new BasicDBObject("_id", -1)).first();

            if (collectionl1.equals("l1")) {
                Thread.sleep(5000);

                    String zonal1 =  dl1.getString("Zona");
                    String sensorl1 =  dl1.getString("Sensor");
                    String datal1 =  dl1.getString("Data").replace("T", " ").replace("Z", " ");

                    String medicaol1 = dl1.getString("Medicao");
                    stringDocumentl1 = zonal1 + "|" + sensorl1 + "|" + datal1 + "|" + medicaol1;

                    byte[] arrayBytel1 = stringDocumentl1.getBytes();

                    MqttMessage mqttMessagel1 = new MqttMessage(arrayBytel1);
                    mqttMessagel1.setQos(0);
                    mqttMessagel1.setRetained(false);
                    mqttClient.publish(topicol1, mqttMessagel1);
                    System.out.println(mqttMessagel1.toString());
             } else {
                return;
            }



        // Tratamento da coleção l1 local
        localcollectionl2 = localMongoDatabase.getCollection(collectionl2);
        Document dl2= localcollectionl2.find().sort(new BasicDBObject("_id", -1)).first();

            if (collectionl2.equals("l2")) {
                Thread.sleep(5000);

                    String zonal2 = dl2.getString("Zona");
                    String sensorl2 = dl2.getString("Sensor");
                    String datal2 = dl2.getString("Data").replace("T", " ").replace("Z", " ");

                    String medicaol2 = dl2.getString("Medicao");
                    stringDocumentl2 = zonal2 + "|" + sensorl2 + "|" + datal2 + "|" + medicaol2;
                    byte[] arrayBytel2 = stringDocumentl2.getBytes();

                    MqttMessage mqttMessagel2 = new MqttMessage(arrayBytel2);
                    mqttMessagel2.setQos(0);
                    mqttMessagel2.setRetained(false);
                    mqttClient.publish(topicol2, mqttMessagel2);
                    System.out.println(mqttMessagel2.toString());

            } else {
                return;
            }



    }
    public static void main(String args[]) throws InterruptedException, MqttPersistenceException, MqttException {

        // verificar se jÃ¡ existem dados no mysql

        // se nÃ£o existirem enviar 1 doc de cada sensor


        MongoToMQTTTest mtm = new MongoToMQTTTest();

        mtm.connectToMQTT();

        //Conexão ao MQTT
        while (true) {
            // obter doc mais recente de cada sensor comparar com a data em memÃ³ria
            mtm.migrate();



            // converter mediÃ§Ã£o de cada sensor em string

            // enviar para o topico

            // guardar data de cada medicao enviada na vairiavel data
        }

    }

}


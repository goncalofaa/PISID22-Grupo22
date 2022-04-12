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

    private static String databaselocal = "medicoes";

    // PERGUNTAR OU VER SE PODEMOS USAR ESTE QUE Ã‰ O DO PROF FÃ¡bio
    private String cloudServer = "tcp://broker.mqtt-dashboard.com:1883";

    // Url para aceder ás réplicas locais
    String urlLocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replicaSet=medicoespisid";

    private String topicot1 = "G22T1";
    private String topicot2 = "G22T1";
    private String topicoh1 = "G22H1";
    private String topicoh2 = "G22H2";
    private String topicol1 = "G22L1";
    private String topicol2 = "G22L2";

    // locais
    private static String collectiont1 = "t1";
    private static String collectiont2 = "t2";
    private static String collectionh1 = "h1";
    private static String collectionh2 = "h2";
    private static String collectionl1 = "l1";
    private static String collectionl2 = "l2";

    // coleÃ§Ãµes locais
    private static MongoCollection<Document> localcollectiont1;
    private static MongoCollection<Document> localcollectiont2;
    private static MongoCollection<Document> localcollectionh1;
    private static MongoCollection<Document> localcollectionh2;
    private static MongoCollection<Document> localcollectionl1;
    private static MongoCollection<Document> localcollectionl2;

    //Últimas datas de cada coleção
    private static String datat1;
    private static String datat2;
    private static String datah1;
    private static String datah2;
    private static String datal1;
    private static String datal2;

    MongoClient localClient = new MongoClient(new MongoClientURI(urlLocal));
    MongoDatabase localMongoDatabase = localClient.getDatabase(databaselocal);
    IMqttClient mqttClient;

    public void connectToMQTT() throws MqttException {

        String clientId = UUID.randomUUID().toString();
        mqttClient = new MqttClient(cloudServer, clientId);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);
        mqttClient.connect(options);
    }

    public void migrate(String lastDate) throws MqttPersistenceException, MqttException, InterruptedException {

        // Tratamento da coleção t1 local
        localcollectiont1 = localMongoDatabase.getCollection(collectiont1);
        FindIterable<Document> newLocalMeasurementst1;
        if (lastDate == null) {
            newLocalMeasurementst1 = localcollectiont1.find();
        } else {
            newLocalMeasurementst1 = localcollectiont1.find(Filters.gt("Data", lastDate));
        }
        for (Document dt1 : newLocalMeasurementst1) {
            if (collectiont1.equals("t1")) {
                String zonat1 = dt1.getString("Zona");
                String sensort1 = dt1.getString("Sensor");
                datat1 = dt1.getString("Data").replace("T", " ").replace("Z", " ").replace("-", "").replace(":", "");
                String medicaot1 = dt1.getString("Medicao");
                String stringDocumentt1 = zonat1 + "|" + sensort1 + "|" + datat1 + "|" + medicaot1;
                byte[] arrayBytet1 = stringDocumentt1.getBytes();
                MqttMessage mqttMessaget1 = new MqttMessage(arrayBytet1);
                mqttMessaget1.setQos(0);
                mqttMessaget1.setRetained(false);
                mqttClient.publish(topicot1, mqttMessaget1);
                System.out.println(mqttMessaget1.toString());
            } else {
                return;
            }
        }
        // Tratamento da coleção t2 local
        localcollectiont2 = localMongoDatabase.getCollection(collectiont2);
        FindIterable<Document> newLocalMeasurementst2;
        if (lastDate == null) {
            newLocalMeasurementst2 = localcollectiont2.find();
        } else {
            newLocalMeasurementst2 = localcollectiont2.find(Filters.gt("Data", lastDate));
        }
        for (Document dt2 : newLocalMeasurementst1) {
            if (collectiont2.equals("t2")) {
                String zonat2 = dt2.getString("Zona");
                String sensort2 = dt2.getString("Sensor");
                datat2 = dt2.getString("Data").replace("T", " ").replace("Z", " ").replace("-", "").replace(":", "");
                String medicaot2 = dt2.getString("Medicao");
                String stringDocumentt2 = zonat2 + "|" + sensort2 + "|" + datat2 + "|" + medicaot2;

                byte[] arrayBytet2 = stringDocumentt2.getBytes();
                MqttMessage mqttMessaget2 = new MqttMessage(arrayBytet2);
                mqttMessaget2.setQos(0);
                mqttMessaget2.setRetained(false);
                mqttClient.publish(topicot2, mqttMessaget2);
                System.out.println(mqttMessaget2.toString());
            } else {
                return;
            }
        }
        // Tratamento da coleção h1 local
        localcollectionh1 = localMongoDatabase.getCollection(collectionh1);
        FindIterable<Document> newLocalMeasurementsh1;
        if (lastDate == null) {
            newLocalMeasurementsh1 = localcollectionh1.find();
        } else {
            newLocalMeasurementsh1 = localcollectionh1.find(Filters.gt("Data", lastDate));
        }
        for (Document dh1 : newLocalMeasurementsh1) {
            if (collectionh1.equals("h1")) {
                String zonah1 = dh1.getString("Zona");
                String sensorh1 = dh1.getString("Sensor");
                datah1 = dh1.getString("Data").replace("T", " ").replace("Z", " ").replace("-", "").replace(":", "");
                String medicaoh1 = dh1.getString("Medicao");
                String stringDocumenth1 = zonah1 + "|" + sensorh1 + "|" + datah1 + "|" + medicaoh1;
                byte[] arrayByteh1 = stringDocumenth1.getBytes();
                MqttMessage mqttMessageh1 = new MqttMessage(arrayByteh1);
                mqttMessageh1.setQos(0);
                mqttMessageh1.setRetained(false);
                mqttClient.publish(topicoh1, mqttMessageh1);
                System.out.println(mqttMessageh1.toString());
            } else {
                return;
            }
        }
        // Tratamento da coleção h2 local
        localcollectionh2 = localMongoDatabase.getCollection(collectionh2);
        FindIterable<Document> newLocalMeasurementsh2;
        if (lastDate == null) {
            newLocalMeasurementsh2 = localcollectionh2.find();
        } else {
            newLocalMeasurementsh2 = localcollectionh2.find(Filters.gt("Data", lastDate));
        }
        for (Document dh2 : newLocalMeasurementsh2) {
            if (collectionh2.equals("h2")) {
                String zonah2 = dh2.getString("Zona");
                String sensorh2 = dh2.getString("Sensor");
                datah2 = dh2.getString("Data").replace("T", " ").replace("Z", " ");
                String medicaoh2 = dh2.getString("Medicao");
                String stringDocumenth2 = zonah2 + "|" + sensorh2 + "|" + datah2 + "|" + medicaoh2;
                byte[] arrayByteh2 = stringDocumenth2.getBytes();
                MqttMessage mqttMessageh2 = new MqttMessage(arrayByteh2);
                mqttMessageh2.setQos(0);
                mqttMessageh2.setRetained(false);
                mqttClient.publish(topicoh2, mqttMessageh2);
                System.out.println(mqttMessageh2.toString());
             } else {
                return;
            }
        }
        // Tratamento da coleção l1 local
        localcollectionl1 = localMongoDatabase.getCollection(collectionl1);
        FindIterable<Document> newLocalMeasurementsl1;
        if (lastDate == null) {
            newLocalMeasurementsl1 = localcollectionl1.find();
        } else {
            newLocalMeasurementsl1 = localcollectionl1.find(Filters.gt("Data", lastDate));
        }
        for (Document dl1 : newLocalMeasurementsl1) {
            if (collectionl1.equals("l1")) {
                String zonal1 = dl1.getString("Zona");
                String sensorl1 = dl1.getString("Sensor");
                datal1 = dl1.getString("Data").replace("T", " ").replace("Z", " ");
                String medicaol1 = dl1.getString("Medicao");
                String stringDocumentl1 = zonal1 + "|" + sensorl1 + "|" + datal1 + "|" + medicaol1;
                byte[] arrayBytel1 = stringDocumentl1.getBytes();
                MqttMessage mqttMessagel1 = new MqttMessage(arrayBytel1);
                mqttMessagel1.setQos(0);
                mqttMessagel1.setRetained(false);
                mqttClient.publish(topicol1, mqttMessagel1);
                System.out.println(mqttMessagel1.toString());
            } else {
                return;
            }
        }
        // Tratamento da coleção l1 local
        localcollectionl2 = localMongoDatabase.getCollection(collectionl2);
        FindIterable<Document> newLocalMeasurementsl2;
        if (lastDate == null) {
            newLocalMeasurementsl2 = localcollectionl2.find();
        } else {
            newLocalMeasurementsl2 = localcollectionl2.find(Filters.gt("Data", lastDate));
        }
        for (Document dl2 : newLocalMeasurementsl2) {
            if (collectionl2.equals("l2")) {
                String zonal2 = dl2.getString("Zona");
                String sensorl2 = dl2.getString("Sensor");
                datal2 = dl2.getString("Data").replace("T", " ").replace("Z", " ");
                String medicaol2 = dl2.getString("Medicao");
                String stringDocumentl2 = zonal2 + "|" + sensorl2 + "|" + datal2 + "|" + medicaol2;
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

    }
    public static void main(String args[]) throws InterruptedException, MqttPersistenceException, MqttException {

        // verificar se jÃ¡ existem dados no mysql

        // se nÃ£o existirem enviar 1 doc de cada sensor


        MongoToMQTTTest mtm = new MongoToMQTTTest();

        mtm.connectToMQTT();

        //Conexão ao MQTT
        while (true) {
            // obter doc mais recente de cada sensor comparar com a data em memÃ³ria
            mtm.migrate(datat1);
            mtm.migrate(datat2);
            mtm.migrate(datah1);
            mtm.migrate(datah2);
            mtm.migrate(datal1);
            mtm.migrate(datah2);
            // converter mediÃ§Ã£o de cada sensor em string

            // enviar para o topico

            // guardar data de cada medicao enviada na vairiavel data
        }

    }

}


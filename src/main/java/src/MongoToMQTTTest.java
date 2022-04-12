package src;

import com.mongodb.MongoClientURI;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.eclipse.paho.client.mqttv3.*;


import org.bson.Document;
import java.util.UUID;

public class MongoToMQTTTest {

    private static String databaselocal = "medicoes";

    // PERGUNTAR OU VER SE PODEMOS USAR ESTE QUE Ã‰ O DO PROF FÃ¡bio
   // private String cloudServer = "tcp://broker.mqtt-dashboard.com:1883";

    private String cloudServer = "tcp://broker.hivemq.com:1883";

    // Url para aceder ás réplicas locais
    String urlLocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replSet=replicaimdb";

    private String topicot1 = "G22T1";
    private String topicot2 = "G22T2";
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

    public void migrate(String sensorId, String lastDate) throws MqttPersistenceException, MqttException {

        MongoCollection<Document> localCollection = localMongoDatabase.getCollection(sensorId);

        FindIterable<Document> newLocalMeasurements;
        if (lastDate == null) {
            newLocalMeasurements = localCollection.find();
        } else {
            newLocalMeasurements = localCollection.find(Filters.gt("Data", lastDate));
        }

        for (Document d : newLocalMeasurements) {
             String rawMsg = d.toJson();
             byte[] payload = rawMsg.getBytes();

            MqttMessage msg = new MqttMessage(payload);
            msg.setQos(0);
            msg.setRetained(false);
            if (sensorId.equals("t1")) {
                datat1 = d.getString("Data");
                mqttClient.publish(topicot1, msg);

            } else if (sensorId.equals("t2")) {
                datat2 = d.getString("Data");
                mqttClient.publish(topicot2, msg);
            } else if (sensorId.equals("h1")) {
                datah1 = d.getString("Data");
                mqttClient.publish(topicoh1, msg);
            } else if (sensorId.equals("h2")) {
                datah2 = d.getString("Data");
                mqttClient.publish(topicoh2, msg);
            } else if (sensorId.equals("l1")) {
                datal1 = d.getString("Data");
                mqttClient.publish(topicol1, msg);
            } else if (sensorId.equals("l2")) {
                datal2 = d.getString("Data");
                mqttClient.publish(topicol2, msg);
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
            mtm.migrate(collectiont1,datat1);
            mtm.migrate(collectiont2,datat2);
            mtm.migrate(collectionh1,datah1);
            mtm.migrate(collectionh2,datah2);
            mtm.migrate(collectionl1,datal1);
            mtm.migrate(collectionl2,datal2);
            // converter mediÃ§Ã£o de cada sensor em string

            // enviar para o topico

            // guardar data de cada medicao enviada na vairiavel data
        }

    }

}


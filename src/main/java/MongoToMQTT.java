import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.eclipse.paho.client.mqttv3.*;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
//Mongo to mqtt coleções vazias leva a erro
public class MongoToMQTT {

    private static String urllocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replicaSet=medicoespisid";

    private static String databaselocal = "medicoes";

    //PERGUNTAR OU VER SE PODEMOS USAR ESTE QUE É O DO PROF Fábio
    private static String cloudServer = "tcp://broker.mqtt-dashboard.com:1883";

    //tópico para cada coleção
    private static String topicot1 = "G22T1";
    private static String topicot2 = "G22T1";
    private static String topicoh1 = "G22H1";
    private static String topicoh2 = "G22H2";
    private static String topicol1 = "G22L1";
    private static String topicol2 = "G22L2";

    //locais
    private static String collectiont1 = "t1";
    private static String collectiont2 = "t2";
    private static String collectionh1 = "h1";
    private static String collectionh2 = "h2";
    private static String collectionl1 = "l1";
    private static String collectionl2 = "l2";

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

    //coleções locais
    private static MongoCollection<Document> localcollectiont1;
    private static MongoCollection<Document> localcollectiont2;
    private static MongoCollection<Document> localcollectionh1;
    private static MongoCollection<Document> localcollectionh2;
    private static MongoCollection<Document> localcollectionl1;
    private static MongoCollection<Document> localcollectionl2;

    private static Document  t1aux;
    private static Document t2aux;
    private static Document h1aux;
    private static Document h2aux;
    private static Document l1aux;
    private static Document l2aux;

    private static String auxmsgt1;
    private static String auxmsgt2;
    private static String auxmsgh1;
    private static String auxmsgh2;
    private static String auxmsgl1;
    private static String auxmsgl2;

    private static MqttMessage msgt1;
    private static MqttMessage msgt2;
    private static MqttMessage msgh1;
    private static MqttMessage msgh2;
    private static MqttMessage msgl1;
    private static MqttMessage msgl2;

    private static MongoClient localMongoClient;

    private static MongoDatabase localMongoDatabase;


    private IMqttClient mqttClient;
    public static void main(String args[]) throws InterruptedException, MqttException {
        localMongoClient = new MongoClient(new MongoClientURI(urllocal));
        //verificar se já existem dados no mysql

        //se não existirem enviar 1 doc de cada sensor
        String clientId = UUID.randomUUID().toString();
        IMqttClient mqttClient = new MqttClient(cloudServer,clientId);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(15);
        mqttClient.connect(options);

        localMongoDatabase = localMongoClient.getDatabase(databaselocal);
        localcollectiont1 = localMongoDatabase.getCollection(collectiont1);
        localcollectionh1 = localMongoDatabase.getCollection(collectionh1);
        localcollectionl1 = localMongoDatabase.getCollection(collectionl1);
        localcollectiont2 = localMongoDatabase.getCollection(collectiont2);
        localcollectionh2 = localMongoDatabase.getCollection(collectionh2);
        localcollectionl2 = localMongoDatabase.getCollection(collectionl2);

        t1aux = localcollectiont1.find().sort(new BasicDBObject("_id", -1)).first();
        t2aux = localcollectiont2.find().sort(new BasicDBObject("_id", -1)).first();
        h1aux = localcollectionh1.find().sort(new BasicDBObject("_id", -1)).first();
        h2aux = localcollectionh2.find().sort(new BasicDBObject("_id", -1)).first();
        l1aux = localcollectionl1.find().sort(new BasicDBObject("_id", -1)).first();
        l2aux = localcollectionl2.find().sort(new BasicDBObject("_id", -1)).first();

        auxmsgt1 = (String) t1aux.get("Zona")+"|"+(String) t1aux.get("Sensor")+"|"+(String) t1aux.get("Data")+"|"+(String) t1aux.get("Medicao");
        auxmsgt2 = (String) t2aux.get("Zona")+"|"+(String) t2aux.get("Sensor")+"|"+(String) t2aux.get("Data")+"|"+(String) t2aux.get("Medicao");
        auxmsgh1 = (String) h1aux.get("Zona")+"|"+(String) h1aux.get("Sensor")+"|"+(String) h1aux.get("Data")+"|"+(String) h1aux.get("Medicao");
        auxmsgh2 = (String) h2aux.get("Zona")+"|"+(String) h2aux.get("Sensor")+"|"+(String) h2aux.get("Data")+"|"+(String) h2aux.get("Medicao");
        auxmsgl1 = (String) l1aux.get("Zona")+"|"+(String) l1aux.get("Sensor")+"|"+(String) l1aux.get("Data")+"|"+(String) l1aux.get("Medicao");
        auxmsgl2 = (String) l2aux.get("Zona")+"|"+(String) l2aux.get("Sensor")+"|"+(String) l2aux.get("Data")+"|"+(String) l2aux.get("Medicao");


        msgt1 = new MqttMessage(auxmsgt1.getBytes());
        msgt2 = new MqttMessage(auxmsgt2.getBytes());
        msgh1 = new MqttMessage(auxmsgh1.getBytes());
        msgh2 = new MqttMessage(auxmsgh2.getBytes());
        msgl1 = new MqttMessage(auxmsgl1.getBytes());
        msgl2 = new MqttMessage(auxmsgl2.getBytes());

        msgt1.setQos(0);
        msgt2.setQos(0);
        msgl1.setQos(0);
        msgl2.setQos(0);
        msgh1.setQos(0);
        msgh2.setQos(0);

        msgt1.setRetained(false);
        msgt2.setRetained(false);
        msgl1.setRetained(false);
        msgl2.setRetained(false);
        msgh1.setRetained(false);
        msgh2.setRetained(false);

        mqttClient.publish(topicot1,msgt1);
        mqttClient.publish(topicot2,msgt2);
        mqttClient.publish(topicol1,msgl1);
        mqttClient.publish(topicol2,msgl2);
        mqttClient.publish(topicoh1,msgh1);
        mqttClient.publish(topicoh2,msgh2);

        System.out.println(auxmsgt1);
        System.out.println(auxmsgt2);
        System.out.println(auxmsgl1);
        System.out.println(auxmsgl2);
        System.out.println(auxmsgh1);
        System.out.println(auxmsgh2);


        TimeUnit.SECONDS.sleep(5);
        while(true){

            localMongoDatabase = localMongoClient.getDatabase(databaselocal);

            localcollectiont1 = localMongoDatabase.getCollection(collectiont1);

            localcollectionh1 = localMongoDatabase.getCollection(collectionh1);
            localcollectionl1 = localMongoDatabase.getCollection(collectionl1);
            localcollectiont2 = localMongoDatabase.getCollection(collectiont2);
            localcollectionh2 = localMongoDatabase.getCollection(collectionh2);
            localcollectionl2 = localMongoDatabase.getCollection(collectionl2);

            t1aux = localcollectiont1.find().sort(new BasicDBObject("_id", -1)).first();
            t2aux = localcollectiont2.find().sort(new BasicDBObject("_id", -1)).first();
            h1aux = localcollectionh1.find().sort(new BasicDBObject("_id", -1)).first();
            h2aux = localcollectionh2.find().sort(new BasicDBObject("_id", -1)).first();
            l1aux = localcollectionl1.find().sort(new BasicDBObject("_id", -1)).first();
            l2aux = localcollectionl2.find().sort(new BasicDBObject("_id", -1)).first();

            if(Utilies.isBiggerThanSensorT((String) t1aux.get("Data"),datat1)){
                auxmsgt1 = (String) t1aux.get("Zona")+"|"+(String) t1aux.get("Sensor")+"|"+(String) t1aux.get("Data")+"|"+(String) t1aux.get("Medicao");
                msgt1 = new MqttMessage(auxmsgt1.getBytes());
                msgt1.setQos(0);
                msgt1.setRetained(false);
                mqttClient.publish(topicot1,msgt1);
                System.out.println(auxmsgt1);
            }else {
                System.out.println("T1 não enviou");
            }

            if(Utilies.isBiggerThanSensorT((String) t2aux.get("Data"),datat2)){
                auxmsgt2 = (String) t2aux.get("Zona")+"|"+(String) t2aux.get("Sensor")+"|"+(String) t2aux.get("Data")+"|"+(String) t2aux.get("Medicao");
                msgt2 = new MqttMessage(auxmsgt2.getBytes());
                msgt2.setQos(0);
                msgt2.setRetained(false);
                mqttClient.publish(topicot2,msgt2);
                System.out.println(auxmsgt2);
            }else {
                System.out.println("T2 não enviou");
            }

            if(Utilies.isBiggerThanSensorT((String) h1aux.get("Data"),datah1)){
                auxmsgh1 = (String) h1aux.get("Zona")+"|"+(String) h1aux.get("Sensor")+"|"+(String) h1aux.get("Data")+"|"+(String) h1aux.get("Medicao");
                msgh1 = new MqttMessage(auxmsgh1.getBytes());
                msgh1.setQos(0);
                msgh1.setRetained(false);
                mqttClient.publish(topicoh1,msgh1);
                System.out.println(auxmsgh1);
            }else {
                System.out.println("H1 não enviou");
            }

            if(Utilies.isBiggerThanSensorT((String) h2aux.get("Data"),datah2)){
                auxmsgh2 = (String) h2aux.get("Zona")+"|"+(String) h2aux.get("Sensor")+"|"+(String) h2aux.get("Data")+"|"+(String) h2aux.get("Medicao");
                msgh2 = new MqttMessage(auxmsgh2.getBytes());
                msgh2.setQos(0);
                msgh2.setRetained(false);
                mqttClient.publish(topicoh2,msgh2);
                System.out.println(auxmsgh2);
            }else {
                System.out.println("H2 não enviou");
            }

            if(Utilies.isBiggerThanSensorT((String) l1aux.get("Data"),datal1)){
                auxmsgl1 = (String) l1aux.get("Zona")+"|"+(String) l1aux.get("Sensor")+"|"+(String) l1aux.get("Data")+"|"+(String) l1aux.get("Medicao");
                msgl1 = new MqttMessage(auxmsgl1.getBytes());
                msgl1.setQos(0);
                msgl1.setRetained(false);
                mqttClient.publish(topicol1,msgl1);
                System.out.println(auxmsgl1);
            }else {
                System.out.println("L1 não enviou");
            }

            if(Utilies.isBiggerThanSensorT((String) l2aux.get("Data"),datal2)){
                auxmsgl2 = (String) l2aux.get("Zona")+"|"+(String) l2aux.get("Sensor")+"|"+(String) l2aux.get("Data")+"|"+(String) l2aux.get("Medicao");
                msgl2 = new MqttMessage(auxmsgl2.getBytes());
                msgl2.setQos(0);
                msgl2.setRetained(false);
                mqttClient.publish(topicol2,msgl2);
                System.out.println(auxmsgl2);
            }else {
                System.out.println("L2 não enviou");
            }

            TimeUnit.SECONDS.sleep(5);
            //obter doc mais recente de cada sensor comparar com a data em memória

            //converter medição de cada sensor em string

            //enviar para o topico

            //guardar data de cada medicao enviada na variavel data
        }

    }

}
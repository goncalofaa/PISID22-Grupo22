import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.eclipse.paho.client.mqttv3.*;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
//Mongo to mqtt coleções vazias leva a erro
public class MongoToMQTT {

    private static String urllocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replicaSet=replicaimdb";
    private static String databaseMongo = "sid2022";
    
    private IMqttClient mqttClient;
	private String cloudServer = "tcp://broker.mqtt-dashboard.com:1883";
	private String sensort1Topic = collectiont1;

    //locais
    private static String collectiont1 = "sensort1";
    private static String collectiont2 = "sensort2";
    private static String collectionh1 = "sensorh1";
    private static String collectionh2 = "sensorh2";
    private static String collectionl1 = "sensorl1";
    private static String collectionl2 = "sensorl2";
    
    public MongoToMQTT() {
		String clientId = UUID.randomUUID().toString();
		try {
			mqttClient = new MqttClient(cloudServer,clientId);
			MqttConnectOptions options = new MqttConnectOptions();
			options.setAutomaticReconnect(true);
			options.setCleanSession(false);
			options.setConnectionTimeout(60);
			mqttClient.connect(options);
		} catch (MqttException e) {
			e.printStackTrace();
		}
    }
    
    public IMqttClient getMqttClient() {
		return mqttClient;
	}

	public void setMqttClient(IMqttClient mqttClient) {
		this.mqttClient = mqttClient;
	}

	public String getCloudServer() {
		return cloudServer;
	}

	public void setCloudServer(String cloudServer) {
		this.cloudServer = cloudServer;
	}

	public void publishDocument(String collection, MongoDatabase localMongoDatabase) {
		MongoCollection<Document> localCollection = localMongoDatabase.getCollection(collection);
		Document recentDoc = localCollection.find().sort(new BasicDBObject("_id",-1)).first();
		
		String rawMsg = "Zona:" + recentDoc.getString("Zona") + ";" + "Sensor:" +
				recentDoc.getString("Sensor") + ";" + "Data:" + recentDoc.getString("Data") + ";" +
					"Medicao:" + recentDoc.getString("Medicao");
		byte[] payload = rawMsg.getBytes();
		
	    MqttMessage msg = new MqttMessage(payload);
	    msg.setQos(0);
	    msg.setRetained(false);
	    try {
			mqttClient.publish(collection,msg);
		} catch (MqttPersistenceException e) {
			e.printStackTrace();
		} catch (MqttException e) {
			e.printStackTrace();
		}
    }
    
    
	public static void main(String[] args) throws MqttException, InterruptedException {
		
        System.out.println("conexao db local");
        //conexao db local
		MongoClient localMongoClient = new MongoClient(new MongoClientURI(urllocal));
	    MongoDatabase localMongoDatabase = localMongoClient.getDatabase(databaseMongo);
	    
		MongoToMQTT mongoMqtt = new MongoToMQTT();
		while (true) {
			//System.out.println("Publicando os topicos: ");
			mongoMqtt.publishDocument(collectiont1, localMongoDatabase);
			mongoMqtt.publishDocument(collectiont2, localMongoDatabase);
			mongoMqtt.publishDocument(collectionh1, localMongoDatabase);
			mongoMqtt.publishDocument(collectionh2, localMongoDatabase);
			mongoMqtt.publishDocument(collectionl1, localMongoDatabase);
			mongoMqtt.publishDocument(collectionl2, localMongoDatabase);
			
			Thread.sleep(5000);
		}
	}

 
}
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.eclipse.paho.client.mqttv3.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class MongoToMQTT {

    private static String urllocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replicaSet=replicaImdb";
    private static String databaseMongo = "sid2022";
    
    private IMqttClient mqttClient;
	private String cloudServer = "tcp://broker.mqtt-dashboard.com:1883";

    //locais
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
    private static final int TEMPOENVIO = 5;
    private static HashMap<String, LocalDateTime> datas = new HashMap<>(){{
        put("T1", null);
        put("T2", null);
        put("L1", null);
        put("L2", null);
        put("H1", null);
        put("H2", null);
    }};
    
    
    
    public MongoToMQTT() {
		String clientId = UUID.randomUUID().toString();
		try {
			mqttClient = new MqttClient(cloudServer,clientId);
			MqttConnectOptions options = new MqttConnectOptions();
			options.setAutomaticReconnect(true);
			options.setCleanSession(true);
			options.setConnectionTimeout(45);
			options.setKeepAliveInterval(45);
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

	public void publishDocument(String collection, String sensor,LocalDateTime dataSensor,MongoDatabase localMongoDatabase) {

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
			enviaMensagemMqtt(recentDoc, collection,"");
		}else if(datarec1 == null && dataRecenteMongo != null) {
			enviaMensagemMqtt(recentDoc, collection,"");
		}else if(datarec1 != null && dataRecenteMongo != null && !dataRecenteMongo.isAfter(datarec1)){
			enviaMensagemMqtt(recentDoc, collection,"sensorDown");
		}
		datas.put(sensor, dataRecenteMongo);
	}
	
	public LocalDateTime getDatat1() {
		return datat1;
	}

	public LocalDateTime getDatat2() {
		return datat2;
	}

	public LocalDateTime getDatal1() {
		return datal1;
	}

	public LocalDateTime getDatal2() {
		return datal2;
	}

	public LocalDateTime getDatah1() {
		return datah1;
	}

	public LocalDateTime getDatah2() {
		return datah2;
	}

	public void enviaMensagemMqtt(Document recentDoc, String collection, String rawMsg) {
		if(rawMsg.isEmpty()) {
			rawMsg = "Zona:" + recentDoc.getString("Zona") + ";" + "Sensor:" +
					recentDoc.getString("Sensor") + ";" + "Data:" + recentDoc.getString("Data") + ";" +
					"Medicao:" + recentDoc.getString("Medicao");
		}
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
//	public void obterDataMaisRecenteSensores() {
//		LocalDateTime dataRecenteMongo = null;
//		String dataRecMongo = recentDoc.getString("Data");
//		if(dataRecMongo != null && !dataRecMongo.isEmpty()) {
//			dataRecMongo=dataRecMongo.replace("T", " ");
//			dataRecMongo=dataRecMongo.replace("Z", "");
//			dataRecMongo=dataRecMongo.replace("-", " ");
//			dataRecMongo=dataRecMongo.replace(":", " ");
//			String[] datSplit = dataRecMongo.split(" ");
//			dataRecenteMongo = LocalDateTime.of(Integer.parseInt(datSplit[0]), Integer.parseInt(datSplit[1]), Integer.parseInt(datSplit[2]), 
//					Integer.parseInt(datSplit[3]), Integer.parseInt(datSplit[4]), Integer.parseInt(datSplit[5]));
//		}
//
//		if(dataRecenteMedicoesMysql != null && dataRecenteMongo != null && dataRecenteMedicoesMysql.isAfter(dataRecenteMongo)) {
//			//nao envia para mqtt
//		}else if(dataRecenteMedicoesMysql != null && dataRecenteMongo != null && dataRecenteMedicoesMysql.isBefore(dataRecenteMongo)) {
//			//envia para mqtt
//			enviaMensagemMqtt(recentDoc, collection);
//		}else if(dataRecenteMedicoesMysql == null && dataRecenteMongo != null) {
//			//envia para mqtt
//			enviaMensagemMqtt(recentDoc, collection);
//		}
//	}
    
	public static void main(String[] args) throws MqttException, InterruptedException {
		
        System.out.println("conexao db local");
        //conexao db local
		MongoClient localMongoClient = new MongoClient(new MongoClientURI(urllocal));
	    MongoDatabase localMongoDatabase = localMongoClient.getDatabase(databaseMongo);
	    
		MongoToMQTT mongoMqtt = new MongoToMQTT();

		while (true) {
			TimeUnit.SECONDS.sleep(TEMPOENVIO);
			mongoMqtt.publishDocument(collectiont1,"T1",mongoMqtt.getDatat1(),localMongoDatabase);
			mongoMqtt.publishDocument(collectiont2,"T2",mongoMqtt.getDatat2(), localMongoDatabase);
			mongoMqtt.publishDocument(collectionh1,"H1",mongoMqtt.getDatah1(), localMongoDatabase);
			mongoMqtt.publishDocument(collectionh2,"H2",mongoMqtt.getDatah2(), localMongoDatabase);
			mongoMqtt.publishDocument(collectionl1,"L1",mongoMqtt.getDatal1(), localMongoDatabase);
			mongoMqtt.publishDocument(collectionl2,"L2",mongoMqtt.getDatal2(), localMongoDatabase);
			System.out.println(datas.get("T1"));
		}
	}
}
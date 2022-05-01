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
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class MongoToMQTT {

    private static String urllocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replicaSet=replicaimdb";
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
    
    public MongoToMQTT() {
		String clientId = UUID.randomUUID().toString();
		try {
			mqttClient = new MqttClient(cloudServer,clientId);
			MqttConnectOptions options = new MqttConnectOptions();
			options.setAutomaticReconnect(true);
			options.setCleanSession(false);
			options.setConnectionTimeout(45);
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

	public void publishDocument(String collection, String sensor,MongoDatabase localMongoDatabase) {
//		MysqlConnection con = new MysqlConnection();
//		PreparedStatement st;
//		ResultSet rs;
//		LocalDateTime dataRecenteMedicoesMysql = null;
//		try {
//			st = con.getConnectionSQL().prepareStatement("SELECT Data FROM medicao WHERE Sensor = ? ORDER BY IDMedicao DESC LIMIT 1 ");
//			st.setString(1, sensor);
//			rs = st.executeQuery();
//			if(rs != null && rs.next()) {
//				String dat = rs.getString("Data");
//				dat=dat.replace("-", " ");	
//				dat=dat.replace(":", " ");
//				String[] datSplit = dat.split(" ");
//				dataRecenteMedicoesMysql = LocalDateTime.of(Integer.parseInt(datSplit[0]), Integer.parseInt(datSplit[1]), Integer.parseInt(datSplit[2]), 
//						Integer.parseInt(datSplit[3]), Integer.parseInt(datSplit[4]), Integer.parseInt(datSplit[5]));
//			}
//		} catch (SQLException e1) {
//			e1.printStackTrace();
//		}
		
		MongoCollection<Document> localCollection = localMongoDatabase.getCollection(collection);
		Document recentDoc = localCollection.find().sort(new BasicDBObject("_id",-1)).first();
		
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
		enviaMensagemMqtt(recentDoc, collection);
		
    }
	
	public void enviaMensagemMqtt(Document recentDoc, String collection) {
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
			mongoMqtt.publishDocument(collectiont1,"T1",localMongoDatabase);
			mongoMqtt.publishDocument(collectiont2,"T2", localMongoDatabase);
			mongoMqtt.publishDocument(collectionh1,"H1", localMongoDatabase);
			mongoMqtt.publishDocument(collectionh2,"H2", localMongoDatabase);
			mongoMqtt.publishDocument(collectionl1,"L1", localMongoDatabase);
			mongoMqtt.publishDocument(collectionl2,"L2", localMongoDatabase);
			
			Thread.sleep(5000);
		}
	}
}
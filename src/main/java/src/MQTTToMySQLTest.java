package src;

import org.eclipse.paho.client.mqttv3.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import org.json.JSONObject;

public class MQTTToMySQLTest implements MqttCallback{


    // MQTT
    static String cloudServer = "tcp://broker.mqtt-dashboard.com:1883";
    static String mqttopict1 = "G22T1";
    static String mqttopict2 = "G22T2";
    static String mqttopich1 = "G22H1";
    static String mqttopich2 = "G22H2";
    static String mqttopicl1 = "G22L1";
    static String mqttopicl2 = "G22L2";

    static IMqttClient mqttClient;
    // MySQL
    static String db = "monitorizacao";
    static String DBuser = "root";
    static String DBpass = "";
    static DateFormat dateFormatSQL = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
    static Connection connectionSQL;
    static BlockingQueue<String> messageQueue = new LinkedBlockingDeque<String>();

    public static void main(String[] args)
            throws InterruptedException, MqttSecurityException, MqttException, ClassNotFoundException, SQLException {
        // Class.forName("com.mysql.cj.jdbc.Driver");
        connectionSQL = DriverManager
                .getConnection("jdbc:mysql://localhost/" + db + "?useTimezone=true&serverTimezone=UTC", DBuser, DBpass);

        String clientId = UUID.randomUUID().toString();
        mqttClient = new MqttClient(cloudServer, clientId);

        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);
        options.setConnectionTimeout(10);
        mqttClient.connect(options);

        new MQTTToMySQLTest().subscribe();
        System.out.println("SAIR");
        while (true) {
            String arg1 = messageQueue.take();
            JSONObject jsonObject = new JSONObject(arg1.toString());
            String zona = jsonObject.getString("Zona");
            String sensor = jsonObject.getString("Sensor");

            String data = jsonObject.getString("Data").replace('T', ' ').replace('Z', ' ');

            String leitura = jsonObject.getString("Medicao");

            String queryMedicao = "INSERT INTO Medicao VALUES (NULL, " + 1 + ",'" + sensor + "','" + data + "'," + 1
                    + "," + leitura + ");";

            String queryAlerta = null;
            System.out.println(queryMedicao);
            messageQueue.add(arg1.toString());
            connectionSQL.createStatement().executeUpdate(queryMedicao);

        }
    }

    public void subscribe() throws MqttSecurityException, MqttException {
        mqttClient.setCallback(this);
        mqttClient.subscribe(mqttopict1);
        mqttClient.subscribe(mqttopict2);
        mqttClient.subscribe(mqttopich1);
        mqttClient.subscribe(mqttopich2);
        mqttClient.subscribe(mqttopicl1);
        mqttClient.subscribe(mqttopicl2);
    }

    @Override
    public void connectionLost(Throwable arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void messageArrived(String arg0, MqttMessage arg1) throws Exception {
        //System.out.println(arg1.toString());
        messageQueue.add(arg1.toString());
    }
}

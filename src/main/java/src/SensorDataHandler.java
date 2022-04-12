package src;

import org.eclipse.paho.client.mqttv3.*;

import java.util.UUID;

public class SensorDataHandler extends Thread implements MqttCallback{

    private String topicosensor;
    static String cloudServer = "tcp://broker.mqtt-dashboard.com:1883";
    static IMqttClient mqttClient;


    public SensorDataHandler(String topicosensor){
        this.topicosensor=topicosensor;
    }

    public void run() {
        System.out.println("Comecei "+topicosensor);
        String clientId = UUID.randomUUID().toString();
        try {
            mqttClient = new MqttClient(cloudServer,clientId);
            MqttConnectOptions options = new MqttConnectOptions();
            options.setAutomaticReconnect(true);
            options.setCleanSession(true);
            options.setConnectionTimeout(10);
            mqttClient.connect(options);
            new SensorDataHandler(this.topicosensor).subscribe();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    private void subscribe() throws MqttException {
        mqttClient.setCallback(this);
        mqttClient.subscribe(this.topicosensor);
    }

    @Override
    public void connectionLost(Throwable throwable) {

    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
        System.out.println(mqttMessage.toString());
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
    }
}

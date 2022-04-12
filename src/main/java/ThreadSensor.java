import java.sql.SQLException;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;

public class ThreadSensor extends Thread{
	private MysqlConnection msConnection;
	private MqttReceiver mqttReceiver;
	private String sensorCollection;
	
	
	public ThreadSensor(MysqlConnection msConnection, String sensorCollection) {
		this.msConnection = msConnection;
		this.sensorCollection = sensorCollection;
		mqttReceiver = new MqttReceiver(sensorCollection);
		try {
			mqttReceiver.subscribe();
		} catch (MqttSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


	@Override
    public void run() {
        //noinspection InfiniteLoopStatement
        while (true) {
    		try {
    			String message = mqttReceiver.getMessage();
    			System.out.println(message);
    			if(message != null && !message.isEmpty()){
	    			String[] messageSplit = message.split(";");
	    			String zona = messageSplit[0].split(":")[1].replace("Z", "");
	    			String sensor = messageSplit[1].split(":")[1];
	    			String dataHora  = messageSplit[2].replace("Data:","");
	    			dataHora = dataHora.replace("T", ";");
	    			dataHora = dataHora.replace("Z", "");
	    			String anoMesDia = dataHora.split(";")[0];
	    			String hora = dataHora.split(";")[1];
	    			String data = anoMesDia + " " + hora;
	    			String leitura = messageSplit[3].split(":")[1].replace(",", ".");
	    			
	    			String query = "INSERT INTO medicao (Zona, Sensor, Data, Valido, Leitura) VALUES ('" + zona + "','" + sensor + "','" + data + "','" + "1" + "','" + leitura + "');";
	    			System.out.println(query);
	    			msConnection.getConnectionSQL().createStatement().executeUpdate(query);
    			}
    			
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    } 

}

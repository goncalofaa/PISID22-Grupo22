import java.sql.Connection;
import java.sql.SQLException;

public class MqttToMysql {

    //topico para cada colecao
    private static String collectionsensort1 = "sensort1";
    private static String collectionsensort2 = "sensort2";
    private static String collectionsensorh1 = "sensorh1";
    private static String collectionsensorh2 = "sensorh2";
    private static String collectionsensorl1 = "sensorl1";
    private static String collectionsensorl2 = "sensorl2";


    private static String db = "monitorizacao";
    private static String DBuser = "root";
    private static String DBpass = "";
    private static Connection connectionSQL;
    

    public static void main(String args[]){
    	MysqlConnection msConnection = new MysqlConnection();
    	new ThreadSensor(msConnection, collectionsensort1).start();
    	new ThreadSensor(msConnection, collectionsensorh1).start();
    	new ThreadSensor(msConnection, collectionsensorl1).start();
    	new ThreadSensor(msConnection, collectionsensort2).start();
    	new ThreadSensor(msConnection, collectionsensorh2).start();
    	new ThreadSensor(msConnection, collectionsensorl2).start();
    }
}

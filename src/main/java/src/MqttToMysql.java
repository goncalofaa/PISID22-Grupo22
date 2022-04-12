package src;

import java.sql.Connection;

public class MqttToMysql {

    //tópico para cada coleção
    private static String topicot1 = "G22T1";
    private static String topicot2 = "G22T2";
    private static String topicoh1 = "G22H1";
    private static String topicoh2 = "G22H2";
    private static String topicol1 = "G22L1";
    private static String topicol2 = "G22L2";


    private static String db = "monitorizacao";
    private static String DBuser = "root";
    private static String DBpass = "";
    private static Connection connectionSQL;

    public static void main(String args[]){
        SensorDataHandler sdht1 = new SensorDataHandler(topicot1);
        sdht1.start();
      /*  SensorDataHandler sdht2 = new SensorDataHandler(topicot2);
        sdht2.start();
        SensorDataHandler sdhh1 = new SensorDataHandler(topicoh1);
        sdhh1.start();
        SensorDataHandler sdhh2 = new SensorDataHandler(topicoh2);
        sdhh2.start();
        SensorDataHandler sdhl1 = new SensorDataHandler(topicol1);
        sdhl1.start();
        SensorDataHandler sdhl2 = new SensorDataHandler(topicol2);
        sdhl2.start();
    */
    }

}

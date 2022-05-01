import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MqttToMysql {

    //topico para cada colecao
    private static String collectionsensort1 = "sensort1";
    private static String collectionsensort2 = "sensort2";
    private static String collectionsensorh1 = "sensorh1";
    private static String collectionsensorh2 = "sensorh2";
    private static String collectionsensorl1 = "sensorl1";
    private static String collectionsensorl2 = "sensorl2";

    public static void main(String args[]) throws SQLException{
//    	MysqlConnection msConnection = new MysqlConnection();
//    	new ThreadSensor(msConnection, collectionsensort1).start();
//    	new ThreadSensor(msConnection, collectionsensorh1).start();
//    	new ThreadSensor(msConnection, collectionsensorl1).start();
//    	new ThreadSensor(msConnection, collectionsensort2).start();
//    	new ThreadSensor(msConnection, collectionsensorh2).start();
//    	new ThreadSensor(msConnection, collectionsensorl2).start();
    	MysqlProfConnection connectionLocal = new MysqlProfConnection("monitorizacao", "root", "", "localhost");
    	MysqlProfConnection connectionNuvem = new MysqlProfConnection("sid2022", "aluno", "aluno", "194.210.86.10");
    	
    	PreparedStatement st = connectionNuvem.getConnectionSQL().prepareStatement("SELECT * FROM sensor");
    	ResultSet rs = st.executeQuery();
		while(rs.next()) {
			if(rs.getInt("idzona") == 1 && rs.getString("tipo").equals("H"))  {
				new ThreadSensor(connectionLocal, collectionsensorh1, rs.getInt("limiteinferior"), rs.getInt("limitesuperior")).start();
			}
			if(rs.getInt("idzona") == 1 && rs.getString("tipo").equals("L"))  {
				new ThreadSensor(connectionLocal, collectionsensorl1, rs.getInt("limiteinferior"), rs.getInt("limitesuperior")).start();
			} 
			if(rs.getInt("idzona") == 1 && rs.getString("tipo").equals("T"))  {
				new ThreadSensor(connectionLocal, collectionsensort1, rs.getInt("limiteinferior"), rs.getInt("limitesuperior")).start();
			} 
			if(rs.getInt("idzona") == 2 && rs.getString("tipo").equals("H"))  {
				new ThreadSensor(connectionLocal, collectionsensorh2, rs.getInt("limiteinferior"), rs.getInt("limitesuperior")).start();
			} 
			if(rs.getInt("idzona") == 2 && rs.getString("tipo").equals("L"))  {
				new ThreadSensor(connectionLocal, collectionsensorl2, rs.getInt("limiteinferior"), rs.getInt("limitesuperior")).start();
			} 
			if(rs.getInt("idzona") == 2 && rs.getString("tipo").equals("T"))  {
				new ThreadSensor(connectionLocal, collectionsensort2, rs.getInt("limiteinferior"), rs.getInt("limitesuperior")).start();
			} 
		}
    }
}

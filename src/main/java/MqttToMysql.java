import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class MqttToMysql {

    //topico para cada colecao
    private static String collectionsensort1 = "sensort1";
    private static String collectionsensort2 = "sensort2";
    private static String collectionsensorh1 = "sensorh1";
    private static String collectionsensorh2 = "sensorh2";
    private static String collectionsensorl1 = "sensorl1";
    private static String collectionsensorl2 = "sensorl2";
    private static final int TEMPO_SISTEMA_EM_BAIXO = 1;
    
    
    static void alertaAplicationDown(MysqlProfConnection connectionLocal) {
    	PreparedStatement st;
    	LocalDateTime dataRecenteMedicoesMysql = null;
		Timestamp dataatualPT = new Timestamp(System.currentTimeMillis());
		try {
			st = connectionLocal.getConnectionSQL().prepareStatement("SELECT * FROM medicao ORDER BY Data DESC LIMIT 1 ");
			ResultSet rs = st.executeQuery();
			System.out.println(dataatualPT);
			if(rs.next()) {
				String data_ultima_medicao = rs.getString("Data");
				data_ultima_medicao=data_ultima_medicao.replace("-", " ");	
				data_ultima_medicao=data_ultima_medicao.replace(":", " ");
				String[] datSplit = data_ultima_medicao.split(" ");
				dataRecenteMedicoesMysql = LocalDateTime.of(Integer.parseInt(datSplit[0]), Integer.parseInt(datSplit[1]), Integer.parseInt(datSplit[2]), 
						Integer.parseInt(datSplit[3]), Integer.parseInt(datSplit[4]), Integer.parseInt(datSplit[5]));
				Timestamp datarecentemysql = Timestamp.valueOf(dataRecenteMedicoesMysql);
				System.out.println(datarecentemysql);
				Timestamp intervaloAviso = new Timestamp(datarecentemysql.getTime() + (TEMPO_SISTEMA_EM_BAIXO * 1000 * 60 )); // vezes 60
				if(dataatualPT.after(intervaloAviso)) {
					String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + "0" + "', '" + "S" + "', '" + dataatualPT + "', '" + "0" + "', '" + "9" + "' ,'" + "Geral" + "','" + "Sistema em baixo"+ "', '" + 1 + "','" +"100"+ "','" + dataatualPT + "');";
					System.err.println(query);
					connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
				}

			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

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
    	
    	
    	alertaAplicationDown(connectionLocal);
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

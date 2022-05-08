import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.concurrent.CyclicBarrier;

public class MqttToMysql {

    //topico para cada colecao
    private static String collectionsensort1 = "sensort1";
    private static String collectionsensort2 = "sensort2";
    private static String collectionsensorh1 = "sensorh1";
    private static String collectionsensorh2 = "sensorh2";
    private static String collectionsensorl1 = "sensorl1";
    private static String collectionsensorl2 = "sensorl2";
    private static final int TEMPO_SISTEMA_EM_BAIXO = 1;
    private static final int NUMBEROFTHREADS = 6;
    
    
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
					String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + "0" + "', '" + "S" + "', '" + dataatualPT + "', '" + "0" + "', '" + "9" + "' ,'" + "Geral" + "','" + "Sistema em baixo"+ "', '" + 1 + "','" +"0"+ "','" + dataatualPT + "');";
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

    	MysqlProfConnection connectionLocal = new MysqlProfConnection("monitorizacao", "root", "", "localhost");
    	MysqlProfConnection connectionNuvem = new MysqlProfConnection("sid2022", "aluno", "aluno", "194.210.86.10");
    	
    	String limiteinferiorh1 = "";
    	String limitesuperiorh1 = "";
    	String limiteinferiorh2 = "";
    	String limitesuperiorh2 = "";
    	String limiteinferiort1 = "";
    	String limitesuperiort1 = "";
    	String limiteinferiort2 = "";
    	String limitesuperiort2 = "";
    	String limiteinferiorl1 = "";
    	String limitesuperiorl1 = "";
    	String limiteinferiorl2 = "";
    	String limitesuperiorl2 = "";
    	
    	alertaAplicationDown(connectionLocal);
    	PreparedStatement st = connectionNuvem.getConnectionSQL().prepareStatement("SELECT * FROM sensor");
    	ResultSet rs = st.executeQuery();
		while(rs.next()) {
			if(rs.getInt("idzona") == 1 && rs.getString("tipo").equals("H"))  {
				limiteinferiorh1 = rs.getString("limiteinferior");
				limitesuperiorh1 = rs.getString("limitesuperior");
			}
			if(rs.getInt("idzona") == 1 && rs.getString("tipo").equals("L"))  {
				limiteinferiorl1 = rs.getString("limiteinferior");
				limitesuperiorl1 = rs.getString("limitesuperior");
			} 
			if(rs.getInt("idzona") == 1 && rs.getString("tipo").equals("T"))  {
				limiteinferiort1 = rs.getString("limiteinferior");
				limitesuperiort1 = rs.getString("limitesuperior");
			} 
			if(rs.getInt("idzona") == 2 && rs.getString("tipo").equals("H"))  {
				limiteinferiorh2 = rs.getString("limiteinferior");
				limitesuperiorh2 = rs.getString("limitesuperior");
			} 
			if(rs.getInt("idzona") == 2 && rs.getString("tipo").equals("L"))  {
				limiteinferiorl2 = rs.getString("limiteinferior");
				limitesuperiorl2 = rs.getString("limitesuperior");
			} 
			if(rs.getInt("idzona") == 2 && rs.getString("tipo").equals("T"))  {
				limiteinferiort2 = rs.getString("limiteinferior");
				limitesuperiort2 = rs.getString("limitesuperior");
			} 
		}
		CyclicBarrier newBarrier = new CyclicBarrier(NUMBEROFTHREADS);
		if(!limiteinferiorh1.isEmpty() && !limitesuperiorh1.isEmpty() && !limiteinferiorl1.isEmpty() && !limitesuperiorl1.isEmpty()
				&& !limiteinferiort1.isEmpty() && !limitesuperiort1.isEmpty() && !limiteinferiorh2.isEmpty() && !limitesuperiorh2.isEmpty() 
				&& !limiteinferiorl2.isEmpty() && !limitesuperiorl2.isEmpty() && !limiteinferiort2.isEmpty() && !limitesuperiort2.isEmpty())  {
			new ThreadSensor(newBarrier,connectionLocal, collectionsensorh1, Double.parseDouble(limiteinferiorh1), Double.parseDouble(limitesuperiorh1),"H1","1").start();
			new ThreadSensor(newBarrier,connectionLocal, collectionsensorl1, Double.parseDouble(limiteinferiorl1), Double.parseDouble(limitesuperiorl1),"L1","1").start();
			new ThreadSensor(newBarrier,connectionLocal, collectionsensort1, Double.parseDouble(limiteinferiort1), Double.parseDouble(limitesuperiort1),"T1","1").start();
			new ThreadSensor(newBarrier,connectionLocal, collectionsensorh2, Double.parseDouble(limiteinferiorh2), Double.parseDouble(limitesuperiorh2),"H2","2").start();
			new ThreadSensor(newBarrier,connectionLocal, collectionsensorl2, Double.parseDouble(limiteinferiorl2), Double.parseDouble(limitesuperiorl2),"L2","2").start();
			new ThreadSensor(newBarrier,connectionLocal, collectionsensort2, Double.parseDouble(limiteinferiort2), Double.parseDouble(limitesuperiort2),"T2","2").start();
		}
    }
}

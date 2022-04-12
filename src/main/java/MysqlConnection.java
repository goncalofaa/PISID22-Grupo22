

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class MysqlConnection {
	
	private String db = "monitorizacao";
	private String DBuser = "root";
	private String DBpass = "";
	private Connection connectionSQL;
	private DateFormat dateFormatSQL;
	
	public MysqlConnection() {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			connectionSQL = DriverManager.getConnection("jdbc:mysql://localhost/" + db + "?useTimezone=true&serverTimezone=UTC", DBuser, DBpass);
			dateFormatSQL = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		}catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public Connection getConnectionSQL() {
		return connectionSQL;
	}

	public void setConnectionSQL(Connection connectionSQL) {
		this.connectionSQL = connectionSQL;
	}

	public DateFormat getDateFormatSQL() {
		return dateFormatSQL;
	}

	public void setDateFormatSQL(DateFormat dateFormatSQL) {
		this.dateFormatSQL = dateFormatSQL;
	}

	public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException {
		
//		String zona = "Z1";
//		String sensor = "T1";
//		String tipo = "TEM";
//		for (;;) {
//			Date now = new Date();
//			String hora = dateFormatSQL.format(now);
//			Random r = new Random();
//			double leitura = r.nextDouble()*15.0 + 10.0;
//			String query = "INSERT INTO Medicao VALUES (NULL, '" + zona + "','" + sensor + "','" + hora + "','" + leitura + "','" + tipo + "');";
//			System.out.println(query);
//			connectionSQL.createStatement().executeUpdate(query);
//			Thread.sleep(2000);
//		}
	}

}

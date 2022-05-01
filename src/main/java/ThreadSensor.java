import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;

public class ThreadSensor extends Thread{
	//private MysqlConnection msConnection;
	private MysqlProfConnection connectionLocal;
	private MysqlProfConnection connectionNuvem;
	private MqttReceiver mqttReceiver;
	private String sensorCollection;
	double limitesensorsuperior;
	double limitesensorinferior;
	
	private PreparedStatement st;
	private ResultSet rs;
	private PreparedStatement st1;
	private ResultSet rs1;
	
	private int sensorZona;
	private char sensorTipo;
	private double margem_outlier;

	List<Integer> outliers = new ArrayList<>();
	String zona;
	String sensor;
	String data;
	
	double auxiliar;
	
	public ThreadSensor(MysqlProfConnection connectionLocal,  String sensorCollection, int limitesensorinferior, int limitesensorsuperior) {
		this.connectionLocal = connectionLocal;
	//	this.connectionNuvem = connectionNuvem;
		this.sensorCollection = sensorCollection;
		this.limitesensorinferior = limitesensorinferior;
		this.limitesensorsuperior = limitesensorsuperior;
		criarlista();
        String sensor_caracteristicas = sensorCollection.substring(sensorCollection.length() - 2);
        sensorTipo = sensor_caracteristicas.charAt(0);
        sensorZona = Integer.valueOf(sensor_caracteristicas.substring(sensor_caracteristicas.length() - 1));
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
	    			zona = messageSplit[0].split(":")[1].replace("Z", "");
	    			sensor = messageSplit[1].split(":")[1];
	    			String dataHora  = messageSplit[2].replace("Data:","");
	    			dataHora = dataHora.replace("T", ";");
	    			dataHora = dataHora.replace("Z", "");
	    			String anoMesDia = dataHora.split(";")[0];
	    			String hora = dataHora.split(";")[1];
	    			data = anoMesDia + " " + hora;
	    			String leitura = messageSplit[3].split(":")[1].replace(",", ".");
	    	        System.out.println("LEITURA: " + leitura);
	    			// Deve estar errado, but i tried
	    			//se calhar não é preciso percurrer sempre isto pq as margens não vao estar sempre a ser alteradas e também a zona é sempre a mesma.
	    			
	    			//Connection toMySql = (Connection) msConnection;
	    			st = connectionLocal.getConnectionSQL().prepareStatement("SELECT * FROM parametrozona");
	    			rs = st.executeQuery();
	    			auxiliar = ultimo_avaliado;
	    			while(rs.next()) {
	    				int idzona =rs.getInt("IDZona");
	    				if (idzona == sensorZona) {
	    					if(sensorTipo == 't') {
	    						margem_outlier = rs.getDouble("MargemOutlierTemperatura");
	    					}
	    					if(sensorTipo == 'l') {
	    						margem_outlier = rs.getDouble("MargemOutlierLuminosidade");
	    					}
	    					if(sensorTipo == 'h') {
	    						margem_outlier = rs.getDouble("MargemOutlierHumidade");
	    						
	    					}
	    				}
	    			}
	    			double dado = arredondamento(Double.parseDouble(leitura)); 
	    			leitura = Double.toString(dado);
	    			verificar_outlier(leitura);
	    			System.out.println("LEITURA COM ARREDONDAMENTO: " + leitura);
	    			//Connection toMySql2 = (Connection) msConnection; //deve dar com a conecção em cima
	    			st = connectionLocal.getConnectionSQL().prepareStatement("SELECT * FROM cultura");
	    			//st.setInt(1, sensorZona);
	    			rs = st.executeQuery();
	    			while(rs.next()) {
	    				//estado inativo não é preciso verificação de alerta
	    				int estado = rs.getInt("Estado");
	    				int idZonaCultura = rs.getInt("Zona");
	    				int idCultura = rs.getInt("IDCultura");
	    			//	System.out.println("O meu IDZONA: " + idZonaCultura);
	    			//	String idCultura1 = String.valueOf(rs.getInt("IDCultura"));
	    			//	System.out.println( "idcultura autal : " + idCultura);
	    				if(estado == 1) {
		    				if(idZonaCultura == sensorZona) {
		    					Cultura cultura = new Cultura();
		    					cultura.setIdCultura(idCultura);
		    					cultura.setIdUtilizador(rs.getInt("IDUtilizador"));
		    					st1 = connectionLocal.getConnectionSQL().prepareStatement("SELECT * FROM parametrocultura WHERE IDParametroCultura = ?"); // não sei se a comparação funciona com string
		    					st1.setInt(1, idCultura);
		    	    			rs1 = st1.executeQuery();
		    	    			rs1.next();
			    				cultura.setAvisoHumMax(rs1.getDouble("AvisoHumidadeMax"));
			    				cultura.setAvisoHumMin(rs1.getDouble("AvisoHumidadeMin"));
			    				cultura.setAvisoLuzMax(rs1.getDouble("AvisoLuminosidadeMax"));
			    				cultura.setAvisoLuzMin(rs1.getDouble("AvisoLuminosidadeMin"));
			    				cultura.setAvisoTempMax(rs1.getDouble("AvisoTemperaturaMax"));
			    				cultura.setAvisoTempMin(rs1.getDouble("AvisoTemperaturaMin"));
			    				cultura.setLimiteHumMax(rs1.getDouble("LimiteHumidadeMax"));
			    				cultura.setLimiteHumMin(rs1.getDouble("LimiteHumidadeMin"));
			    				cultura.setLimiteLuzMax(rs1.getDouble("LimiteLuminosidadeMax"));
			    				cultura.setLimiteLuzMin(rs1.getDouble("LimiteLuminosidadeMin"));
			    				cultura.setLimiteTempMax(rs1.getDouble("LimiteTemperaturaMax"));
			    				cultura.setLimiteTempMin(rs1.getDouble("LimiteTemperaturaMin"));
			    				cultura.setIntervaloAviso(rs1.getInt("IntervaloAviso"));
	//		    				System.out.println("avisoHMax = " + cultura.getAvisoHumMax());
	//		    				System.out.println("limiteTMax = " + cultura.getLimiteTempMax());
	//		    				System.out.println("avisoLMax = " + cultura.getAvisoLuzMax());
			    				verificar_alertas(leitura,cultura);
		    				}
	    				}
	    			}
	    			
	    			String query = "INSERT INTO medicao (Zona, Sensor, Data, Valido, Leitura) VALUES ('" + zona + "','" + sensor + "','" + data + "','" + valido + "','" + leitura + "');";
	    			System.out.println(query);
	    			connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
    			}
    			
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }
	
	double ultimo_valido = -276;
	double ultimo_avaliado;
	int valido;
	
	int countV = 0;
	int countL= 0;
	int countA = 0;
	
	
	public void verificar_outlier(String leitura) throws SQLException {
//		System.out.println(sensorTipo);
//		System.err.println("Foi verificar alertas outliers");
		double dado = Double.parseDouble(leitura); 
		dado = arredondamento(dado);
		if(margem_outlier != 0) { // isto é, ainda não existe um valor atribuído por isso está no default
			System.err.println("Entrei no margem outlier");
			verificar_outliers(dado);
		}
		verificar_lista_outliers(dado);
	}
	
	
	
	public void verificar_alertas(String leitura, Cultura cultura) throws InterruptedException, SQLException {
//		System.out.println(sensorTipo);
//		System.out.println("Foi verificar alertas");
		double dado = Double.parseDouble(leitura); 
		dado = arredondamento(dado);
//		if(margem_outlier != 0) { // isto é, ainda não existe um valor atribuído por isso está no default
//			System.err.println("Entrei no margem outlier");
//			verificar_outliers(dado, cultura);
//		}
//		verificar_lista_outliers();
		if(valido == 0) {
			if(sensorTipo == 't') {
				alertatemperatura(dado, cultura);
			}
			if(sensorTipo == 'l') {
				alertaLuz(dado, cultura);
			}
			if(sensorTipo == 'h') {
//				System.out.println("Entrei aki alerta humidade");
//				System.err.println(dado);
				alertaHumidade(dado, cultura);	
			}
		}
	}
	
	public static double arredondamento(double medicao) {
		BigDecimal decimal = new BigDecimal(medicao).setScale(2, RoundingMode.HALF_EVEN);
		return decimal.doubleValue();
	}
	
	public int verificacao_validade(double medicao) {
		//verificação se os dados estão corretos, provavelmente em trigger
		
		
		
		if(limitesensorsuperior < medicao || limitesensorinferior > medicao) {
			return 1; // inválido
		}else {
			return 0; // válido
		}
	}
	
	public void criarlista() {
		for(int i = 0; i < 12; i++) {
			outliers.add(0);
		}
	}
	
	int count_Outlier = 0;
	
	public void verificar_outliers(double medicao) {  // adicionar aqui o tipo de sensor ??
		if(verificacao_validade(medicao) == 0) {
			if(ultimo_valido == -276) {
				ultimo_valido = medicao;
			}
			double diferenca = Math.abs(ultimo_valido - medicao);
			System.out.println("Diferença - " + diferenca );
			if(count_Outlier >= 2) {
				double diferenca2 = Math.abs(ultimo_avaliado - medicao);
				if(diferenca2 < margem_outlier) {
					ultimo_valido = medicao;
					valido = 0;
					count_Outlier = 0;
				}
				else{ // olhar outra vez, se queremos que o count continue quando passar a 2.
					valido = 2;
					count_Outlier++;
				}
			}
			else if(diferenca > margem_outlier) {
				valido = 2;
				count_Outlier++;
			}else {
				ultimo_valido = medicao;
				valido = 0;
				count_Outlier = 0;
			}
		}else {
			valido = 1;
		}
		ultimo_avaliado = medicao;
		//não há concorrencia por isso acho que pode ficar assim
		System.err.println("valido = " + valido);
		outliers.add(valido);
		outliers.remove(0);
	}
	
	
	public void verificar_lista_outliers(double medicao) throws SQLException { // da para reduzir codigo se for apenas um if
		int count_1 = 0;
		int count_2 = 0;
		for(int i = 0; i < 12; i++) {
			if(outliers.get(i) == 1){
				count_1++;
			}
			else if(outliers.get(i) == 2){
				count_2++;
			}
		}
		if(count_1 >= 2 || count_1 + count_2 >= 4) {
			alertaoutlier(medicao);
		}
	}
	
	
	private void alertaoutlier(double medicao) throws SQLException {
		//envia para o mysql o alerta
		//limpar a lista e criar uma nova
		outliers.clear();
		criarlista();
		String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "7" + "' ,'" + null + "','" + "Alerta Outlier, medição errada do sensor"+ "', '" + 1000 + "','" + 0 + "','" + data + "');";
		System.err.println(query);
		connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
		
	}
	
	private void alertatemperatura(double medicao, Cultura cultura) throws InterruptedException, SQLException { // estamos a enviar todos os avisos, mudar a ordem
		System.err.println("Entrei no alerta e este foi o valor auxiliar: " + auxiliar + "este foi o valor da medicao: " + medicao);
		if((cultura.getLimiteTempMax() < medicao || cultura.getLimiteTempMin() > medicao) && (cultura.getLimiteTempMax() < auxiliar || cultura.getLimiteTempMin() > auxiliar)) {
			alertavermelho(cultura, medicao);
		}
		else if(((cultura.getAvisoTempMax() + cultura.getLimiteTempMax()) / 2 < medicao || (cultura.getAvisoTempMin() + cultura.getLimiteTempMin()) / 2 > medicao) && ((cultura.getAvisoTempMax() + cultura.getLimiteTempMax()) / 2 < auxiliar || (cultura.getAvisoTempMin() + cultura.getLimiteTempMin()) / 2 > auxiliar)) {
			alertalaranja(cultura, medicao);
		}
		else if((cultura.getAvisoTempMax() < medicao || cultura.getAvisoTempMin() > medicao) && (cultura.getAvisoTempMax() < auxiliar || cultura.getAvisoTempMin() > auxiliar)) {
			alertaamarelo(cultura, medicao);
		}
	}
	
	private void alertaHumidade(double medicao, Cultura cultura) throws InterruptedException, SQLException { // estamos a enviar todos os avisos, mudar a ordem
		System.err.println("Entrei no alerta e este foi o valor auxiliar: " + auxiliar + "este foi o valor da medicao: " + medicao);
		if((cultura.getLimiteHumMax() < medicao || cultura.getLimiteHumMin() > medicao) && ( cultura.getLimiteHumMax() < auxiliar || cultura.getLimiteHumMin() > auxiliar)) {
			alertavermelho(cultura, medicao);
		}
		else if(((cultura.getAvisoHumMax() + cultura.getLimiteHumMax()) / 2 < medicao || (cultura.getAvisoHumMin() + cultura.getLimiteHumMin()) / 2 > medicao) && ((cultura.getAvisoHumMax() + cultura.getLimiteHumMax()) / 2 < auxiliar || (cultura.getAvisoHumMin() + cultura.getLimiteHumMin()) / 2 > auxiliar)) {
			alertalaranja(cultura, medicao);
		}
		else if((cultura.getAvisoHumMax() < medicao || cultura.getAvisoHumMin() > medicao) && (cultura.getAvisoHumMax() < auxiliar || cultura.getAvisoHumMin() > auxiliar)) {
			alertaamarelo(cultura, medicao);
		}
	}
	
	private void alertaLuz(double medicao, Cultura cultura) throws InterruptedException, SQLException {
		System.err.println("Entrei no alerta e este foi o valor auxiliar: " + auxiliar + "este foi o valor da medicao: " + medicao);
		if((cultura.getLimiteLuzMax() < medicao || cultura.getLimiteLuzMin() > medicao) && (cultura.getLimiteLuzMax() < auxiliar || cultura.getLimiteLuzMin() > auxiliar)) {
			alertavermelho(cultura, medicao);
		}
		else if(((cultura.getAvisoLuzMax() + cultura.getLimiteLuzMax()) / 2 < medicao || (cultura.getAvisoLuzMin() + cultura.getLimiteLuzMin()) / 2 > medicao) && ((cultura.getAvisoLuzMax() + cultura.getLimiteLuzMax()) / 2 <  auxiliar || (cultura.getAvisoLuzMin() + cultura.getLimiteLuzMin()) / 2 >  auxiliar)) {
			alertalaranja(cultura, medicao);
		}
		else if((cultura.getAvisoLuzMax() < medicao || cultura.getAvisoLuzMin() > medicao) && (cultura.getAvisoLuzMax() < auxiliar || cultura.getAvisoLuzMin() > auxiliar)) {
			alertaamarelo(cultura, medicao);
		}
	}
	
	Timestamp datAmarelo;
	Timestamp IntAmarelo;
	Timestamp datlaranja;
	Timestamp Intlaranja;
	Timestamp datvermelho;
	Timestamp Intvermelho;
	
	private void alertavermelho(Cultura cultura, double medicao) throws InterruptedException, SQLException {
		datAmarelo = null;
		datlaranja = null;
		if(datvermelho == null) {
			datvermelho = new Timestamp(System.currentTimeMillis());
			Intvermelho = new Timestamp(System.currentTimeMillis() + (cultura.getIntervaloAviso() * 1000)); // vezes 60
			String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "3" + "' ,'" + cultura + "','" + "Alerta vermelho do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + data + "');";
			System.err.println(query);
			connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
		}
		else {
			datvermelho = new Timestamp(System.currentTimeMillis());
			if(datvermelho.after(Intvermelho)) {
				String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "3" + "' ,'" + cultura + "','" + "Alerta vermelho do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + data + "');";
				System.err.println(query);
				connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
			Intvermelho = new Timestamp(System.currentTimeMillis() + (cultura.getIntervaloAviso() * 1000));
			}
		}
	}

	private void alertalaranja(Cultura cultura, double medicao) throws InterruptedException, SQLException {
		datAmarelo = null;
		datvermelho = null;
		if(datlaranja == null) {
		
			datlaranja = new Timestamp(System.currentTimeMillis());
			Intlaranja = new Timestamp(System.currentTimeMillis() + (cultura.getIntervaloAviso() * 1000)); // vezes 60
			String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "2" + "' ,'" + cultura + "','" + "Alerta laranja do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + data + "');";
			System.err.println(query);
			connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
		}
		else {
		
			datlaranja = new Timestamp(System.currentTimeMillis());

			if(datlaranja.after(Intlaranja)) {
				String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "2" + "' ,'" + cultura + "','" + "Alerta laranja do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + data + "');";
				System.err.println(query);
				connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
				Intlaranja = new Timestamp(System.currentTimeMillis() + (cultura.getIntervaloAviso() * 1000));
			}
		}
	}
	
	private void alertaamarelo(Cultura cultura, double medicao) throws InterruptedException, SQLException {
		datlaranja = null;
		datvermelho = null;
		if(datAmarelo == null) {
			datAmarelo = new Timestamp(System.currentTimeMillis());
			IntAmarelo = new Timestamp(System.currentTimeMillis() + (cultura.getIntervaloAviso() * 1000)); // vezes 60
			String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "1" + "' ,'" + cultura + "','" + "Alerta amarelo do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + data + "');";
			System.err.println(query);
			connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
		}
		else {
			datAmarelo = new Timestamp(System.currentTimeMillis());
			if(datAmarelo.after(IntAmarelo)) {
				String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "1" + "' ,'" + cultura + "','" + "Alerta amarelo do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + data + "');";
				System.err.println(query);
				connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
				IntAmarelo = new Timestamp(System.currentTimeMillis() + (cultura.getIntervaloAviso() * 1000));
			}
		}
	}
}

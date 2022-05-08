import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

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
	private final int KEEPALIVE = 45;
	private final int INTERVALOALERTA8 = 1;
	private CyclicBarrier sensorsBarrier;

	List<Integer> outliers = new ArrayList<>();
	String zona;
	String sensor;
	String data;
	double ultimo_valido = -276;
	double ultimo_avaliado;
	int valido;
	
	int countV = 0;
	int countL= 0;
	int countA = 0;
	
	double auxiliar;
	
	public ThreadSensor(CyclicBarrier sensorsBarrier, MysqlProfConnection connectionLocal,  String sensorCollection, double limitesensorinferior, double limitesensorsuperior, String sensor, String zona) {
		this.connectionLocal = connectionLocal;
		this.sensorCollection = sensorCollection;
		this.limitesensorinferior = limitesensorinferior;
		this.limitesensorsuperior = limitesensorsuperior;
		this.sensor = sensor;
		this.zona = zona;
		this.sensorsBarrier = sensorsBarrier;
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
		SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd 'at' HH:mm:ss z");
		Date date = new Date(System.currentTimeMillis());
		mqttReceiver.getMessageList().clear();
		while (true) {
    		try {
    			String message = mqttReceiver.getMessage();
				String dataentrada= String.valueOf(new Timestamp(System.currentTimeMillis()));
    			if(!message.equalsIgnoreCase("sensorDown")) {
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
//	    	        System.out.println("LEITURA: " + leitura);
	    			// Deve estar errado, but i tried
	    			//se calhar n�o � preciso percurrer sempre isto pq as margens n�o vao estar sempre a ser alteradas e tamb�m a zona � sempre a mesma.

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

	    			verificar_outlier(leitura, sensor);
//
//	    			System.out.println("LEITURA COM ARREDONDAMENTO: " + leitura);
	    			//Connection toMySql2 = (Connection) msConnection; //deve dar com a conec��o em cima
	    			st = connectionLocal.getConnectionSQL().prepareStatement("SELECT * FROM cultura");
	    			//st.setInt(1, sensorZona);
	    			rs = st.executeQuery();
	    			while(rs.next()) {
	    				//estado inativo n�o � preciso verifica��o de alerta
	    				int estado = rs.getInt("Estado");
	    				int idZonaCultura = rs.getInt("Zona");
	    				int idCultura = rs.getInt("IDCultura");
						String nomecultura = rs.getString("NomeCultura");
						if(estado == 1) {
		    				if(idZonaCultura == sensorZona) {
		    					Cultura cultura = new Cultura();
		    					cultura.setIdCultura(idCultura);
								cultura.setNomeCultura(nomecultura);
								cultura.setIdUtilizador(rs.getInt("IDUtilizador"));
		    					st1 = connectionLocal.getConnectionSQL().prepareStatement("SELECT * FROM parametrocultura WHERE IDParametroCultura = ?"); // n�o sei se a compara��o funciona com string
		    					st1.setInt(1, idCultura);
		    	    			rs1 = st1.executeQuery();
		    	    			if(rs1.next()) {
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
		    	    			}

			    				verificar_alertas(leitura,cultura);
		    				}
	    				}
	    			}
					date = new Date(System.currentTimeMillis());
					System.out.println(message + "| Data Fim Processamento: " + formatter.format(date));
	    			sensorsBarrier.await();
	    			String query = "INSERT INTO medicao (Zona, Sensor, Data, Valido, Leitura) VALUES ('" + zona + "','" + sensor + "','" + data + "','" + valido + "','" + leitura + "');";
	    			//System.out.println(query);
	    			connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
	    			//System.err.println("novo insert " + sensorCollection + "  " + formatter.format(date));

    			}else {
	    			PreparedStatement st3 = connectionLocal.getConnectionSQL().prepareStatement("SELECT * FROM alerta WHERE Sensor = ? and TipoAlerta = ? AND Data >= now() - interval "+INTERVALOALERTA8+" minute ORDER BY Data DESC LIMIT 1");
	    			st3.setString(1, sensor);
	    			st3.setString(2, "8");
	    			ResultSet rs3 = st3.executeQuery();
	    			Timestamp dataatualPT = new Timestamp(System.currentTimeMillis());
	    			if(!rs3.next()) {
	    				String query1 = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + dataatualPT + "', '" + "0" + "', '" + "8" + "' ,'" + "Nao tem" + "','" + "Alerta do tipo: " + "8" + "', '" + "1" + "','" + "0" + "','" + dataatualPT + "');";
						//System.err.println(query1);
						connectionLocal.getConnectionSQL().createStatement().executeUpdate(query1);
	    			}
					sensorsBarrier.await();
    			}
				//System.out.println(message + "| Data Fim Processamento: " + new Timestamp(System.currentTimeMillis()) + " | Data Entrada na Thread: " + dataentrada);
			} catch (SQLException | InterruptedException | BrokenBarrierException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
    }



	public void verificar_outlier(String leitura, String sensor) throws SQLException {
//		System.out.println(sensorTipo);
//		System.err.println("Foi verificar alertas outliers");
		double dado = Double.parseDouble(leitura);
		dado = arredondamento(dado);
		if(margem_outlier != 0) { // isto �, ainda n�o existe um valor atribu�do por isso est� no default
			//System.err.println("Entrei no margem outlier");
			verificar_outliers(dado, sensor);
		}
		verificar_lista_outliers(dado);
	}



	public void verificar_alertas(String leitura, Cultura cultura) throws InterruptedException, SQLException {
//		System.out.println(sensorTipo);
//		System.out.println("Foi verificar alertas");
		double dado = Double.parseDouble(leitura);
		dado = arredondamento(dado);
//		if(margem_outlier != 0) { // isto �, ainda n�o existe um valor atribu�do por isso est� no default
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

	public int verificacao_validade(double medicao, String sensor) {
		//verifica��o se os dados est�o corretos, provavelmente em trigger
		if(limitesensorsuperior < medicao || limitesensorinferior > medicao || (sensor.contains("H") && medicao<0) || (sensor.contains("L") && medicao<0) || (sensor.contains("H") && medicao>100)) {
			return 1; // inv�lido
		}else {
			return 0; // v�lido
		}
	}

	public void criarlista() {
		for(int i = 0; i < 12; i++) {
			outliers.add(0);
		}
	}

	int count_Outlier = 0;

	public void verificar_outliers(double medicao, String sensor) {  // adicionar aqui o tipo de sensor ??
		if(verificacao_validade(medicao, sensor) == 0) {
			if(ultimo_valido == -276) {
				ultimo_valido = medicao;
			}
			double diferenca = Math.abs(ultimo_valido - medicao);
			//System.out.println("Diferen�a - " + diferenca );
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
		//n�o h� concorrencia por isso acho que pode ficar assim
		//System.err.println("valido = " + valido);
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
		String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "7" + "' ,'" + "Cultura Geral" + "','" + "Alerta Outlier, medi��o errada do sensor"+ "', '" + 1 + "','" + 0 + "','" + data + "');";
		//System.err.println(query);
		connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);

	}

	private void alertatemperatura(double medicao, Cultura cultura) throws InterruptedException, SQLException { // estamos a enviar todos os avisos, mudar a ordem
		//System.err.println("Entrei no alerta e este foi o valor auxiliar: " + auxiliar + "este foi o valor da medicao: " + medicao);
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
		//System.err.println("Entrei no alerta e este foi o valor auxiliar: " + auxiliar + "este foi o valor da medicao: " + medicao);
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
		//System.err.println("Entrei no alerta e este foi o valor auxiliar: " + auxiliar + "este foi o valor da medicao: " + medicao);
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
		PreparedStatement st3 = connectionLocal.getConnectionSQL().prepareStatement("SELECT * FROM alerta WHERE Sensor = ? AND IDCultura = ? ORDER BY HoraEscrita DESC LIMIT 1");
		st3.setString(1, sensor);
		st3.setInt(2, cultura.getIdCultura());
		ResultSet rs3 = st3.executeQuery();
		Timestamp dataatualPT = new Timestamp(System.currentTimeMillis());
		Timestamp dataatual = new Timestamp(System.currentTimeMillis() + (1000 * 60 * 60));;
		if(rs3.next()) {
			Timestamp ultimoaviso = rs3.getTimestamp("HoraEscrita");
			int ultimoTipoAviso = rs3.getInt("TipoAlerta");
			if(ultimoTipoAviso == 3) {
				//System.out.println("dataAtual = " + dataatual+ " "+cultura.getIdCultura());
				//System.out.println("ultimo aviso = " + ultimoaviso+ " "+cultura.getIdCultura());
				Timestamp intervaloAviso = new Timestamp(ultimoaviso.getTime() + (cultura.getIntervaloAviso() * 1000 * 60  )); // vezes 60
				//System.out.println("intervalo aviso = " + intervaloAviso + " "+cultura.getIdCultura());
				if(dataatual.after(intervaloAviso)) {
					String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "3" + "' ,'" + cultura.getNomeCultura() + "','" + "Alerta vermelho do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + dataatualPT + "');";
					//System.err.println(query);
					connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
				}
			}
			else {
				String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "3" + "' ,'" + cultura.getNomeCultura() + "','" + "Alerta vermelho do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + dataatualPT + "');";
				//System.err.println(query);
				connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
			}
		}
		else {
			String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "3" + "' ,'" + cultura.getNomeCultura() + "','" + "Alerta vermelho do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + dataatualPT + "');";
			//System.err.println(query);
			connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
		}
	}

	private void alertalaranja(Cultura cultura, double medicao) throws InterruptedException, SQLException {
		PreparedStatement st3 = connectionLocal.getConnectionSQL().prepareStatement("SELECT * FROM alerta WHERE Sensor = ? AND IDCultura = ? ORDER BY HoraEscrita DESC LIMIT 1");
		st3.setString(1, sensor);
		st3.setInt(2, cultura.getIdCultura());
		ResultSet rs3 = st3.executeQuery();
		Timestamp dataatualPT = new Timestamp(System.currentTimeMillis());;
		Timestamp dataatual = new Timestamp(System.currentTimeMillis() + (1000 * 60 * 60));;
		if(rs3.next()) {
			Timestamp ultimoaviso = rs3.getTimestamp("HoraEscrita");
			int ultimoTipoAviso = rs3.getInt("TipoAlerta");
			if(ultimoTipoAviso == 2) {
				//System.out.println("dataAtual = " + dataatual+ " "+cultura.getIdCultura());
				//System.out.println("ultimo aviso = " + ultimoaviso+ " "+cultura.getIdCultura());
				Timestamp intervaloAviso = new Timestamp(ultimoaviso.getTime() + (cultura.getIntervaloAviso() * 1000 * 60)); // vezes 60
				//System.out.println("intervalo aviso = " + intervaloAviso + " "+cultura.getIdCultura());
				if(dataatual.after(intervaloAviso)) {
					String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "2" + "' ,'" + cultura.getNomeCultura() + "','" + "Alerta laranja do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + dataatualPT  + "');";
					//System.err.println(query);
					connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
				}
			}
			else {
				String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "2" + "' ,'" + cultura.getNomeCultura() + "','" + "Alerta laranja do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + dataatualPT  + "');";
				//System.err.println(query);
				connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
			}
		}
		else {
			String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "2" + "' ,'" + cultura.getNomeCultura() + "','" + "Alerta laranja do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + dataatualPT  + "');";
			//System.err.println(query);
			connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
		}
	}

	private void alertaamarelo(Cultura cultura, double medicao) throws InterruptedException, SQLException {
		PreparedStatement st3 = connectionLocal.getConnectionSQL().prepareStatement("SELECT * FROM alerta WHERE Sensor = ? AND IDCultura = ? ORDER BY HoraEscrita DESC LIMIT 1");
		st3.setString(1, sensor);
		st3.setInt(2, cultura.getIdCultura());
		ResultSet rs3 = st3.executeQuery();
		Timestamp dataatualPT = new Timestamp(System.currentTimeMillis());;
		Timestamp dataatual = new Timestamp(System.currentTimeMillis() + (1000 * 60 * 60));
		if(rs3.next()) {
			Timestamp ultimoaviso = rs3.getTimestamp("HoraEscrita");
			int ultimoTipoAviso = rs3.getInt("TipoAlerta");
			if(ultimoTipoAviso == 1) {
				//System.out.println("dataAtual = " + dataatual+ " "+cultura.getIdCultura());
				//System.out.println("ultimo aviso = " + ultimoaviso+ " "+cultura.getIdCultura());
				Timestamp intervaloAviso = new Timestamp(ultimoaviso.getTime() + (cultura.getIntervaloAviso() * 1000 * 60 )); // vezes 60
				//System.out.println("intervalo aviso = " + intervaloAviso + " "+cultura.getIdCultura());
				if(dataatual.after(intervaloAviso)) {
					String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "1" + "' ,'" + cultura.getNomeCultura() + "','" + "Alerta amarelo do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + dataatualPT + "');";
					//System.err.println(query);
					connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
				}
			}
			else {
				String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "1" + "' ,'" + cultura.getNomeCultura() + "','" + "Alerta amarelo do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + dataatualPT + "');";
				//System.err.println(query);
				connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
			}
		}
		else {
			String query = "INSERT INTO alerta (Zona, Sensor, Data, Leitura, TipoAlerta, Cultura, Mensagem, IDUtilizador, IDCultura, HoraEscrita) VALUES ('" + zona + "', '" + sensor + "', '" + data + "', '" + medicao + "', '" + "1" + "' ,'" + cultura.getNomeCultura() + "','" + "Alerta amarelo do tipo: " + sensorTipo + "', '" + cultura.getIdUtilizador() + "','" + cultura.getIdCultura() + "','" + dataatualPT + "');";
			//System.err.println(query);
			connectionLocal.getConnectionSQL().createStatement().executeUpdate(query);
		}
	}
}

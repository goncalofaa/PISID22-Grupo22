package src;

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
    private MysqlConnection msConnection;
    private MqttReceiver mqttReceiver;
    private String sensorCollection;

    private PreparedStatement st;
    private ResultSet rs;

    private int sensorZona;
    private char sensorTipo;
    private double margem_outlier;

    List<Integer> outliers = new ArrayList<>();

    public ThreadSensor(MysqlConnection msConnection, String sensorCollection) {
        this.msConnection = msConnection;
        this.sensorCollection = sensorCollection;
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
                    String zona = messageSplit[0].split(":")[1].replace("Z", "");
                    String sensor = messageSplit[1].split(":")[1];
                    String dataHora  = messageSplit[2].replace("Data:","");
                    dataHora = dataHora.replace("T", ";");
                    dataHora = dataHora.replace("Z", "");
                    String anoMesDia = dataHora.split(";")[0];
                    String hora = dataHora.split(";")[1];
                    String data = anoMesDia + " " + hora;
                    String leitura = messageSplit[3].split(":")[1].replace(",", ".");

                    // Deve estar errado, but i tried
                    //se calhar não é preciso percurrer sempre isto pq as margens não vao estar sempre a ser alteradas e também a zona é sempre a mesma.

                    Connection toMySql = (Connection) msConnection;
                    st = toMySql.prepareStatement("SELECT * FROM parametrozona");
                    rs = st.executeQuery();
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

                    Connection toMySql2 = (Connection) msConnection; //deve dar com a conecção em cima
                    st = toMySql2.prepareStatement("SELECT * FROM cultura");
                    rs = st.executeQuery();
                    while(rs.next()) {
                        int idZonaCultura = rs.getInt("Zona");
                        String idCultura = String.valueOf(rs.getInt("IDCultura"));
                        if(idZonaCultura == sensorZona) {
                            st = toMySql2.prepareStatement("SELECT * FROM parametrocultura WHERE IDParametroCultura = ?"); // não sei se a comparação funciona com string
                            st.setString(1, idCultura);
                            //	st.setint
                            rs = st.executeQuery();
                            Cultura cultura = new Cultura();
                            cultura.setAvisoHumMax(rs.getDouble("AvisoHumidadeMax"));
                            cultura.setAvisoHumMin(rs.getDouble("AvisoHumidadeMin"));
                            cultura.setAvisoLuzMax(rs.getDouble("AvisoLuminosidadeMax"));
                            cultura.setAvisoLuzMin(rs.getDouble("AvisoLuminosidadeMin"));
                            cultura.setAvisoTempMax(rs.getDouble("AvisoTemperaturaMax"));
                            cultura.setAvisoTempMin(rs.getDouble("AvisoTemperaturaMin"));
                            cultura.setLimiteHumMax(rs.getDouble("LimiteHumidadeMax"));
                            cultura.setLimiteHumMin(rs.getDouble("LimiteHumidadeMin"));
                            cultura.setLimiteLuzMax(rs.getDouble("LimiteLuminosidadeMax"));
                            cultura.setLimiteLuzMin(rs.getDouble("LimiteLuminosidadeMin"));
                            cultura.setLimiteTempMax(rs.getDouble("LimiteTemperaturaMax"));
                            cultura.setLimiteTempMin(rs.getDouble("LimiteTemperaturaMin"));
                            cultura.setIntervaloAviso(rs.getInt("IntervaloAviso"));
                            verificar_alertas(leitura,cultura);
                        }
                    }

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

    //falta obter estes valores
    double limitesensorsuperior;
    double limitesensorinferior;

    double ultimo_valido = 20;
    double ultimo_avaliado = 20;
    int valido;

    int countV = 0;
    int countL= 0;
    int countA = 0;

    public void verificar_alertas(String leitura, Cultura cultura) throws InterruptedException {
        double dado = Double.parseDouble(leitura);
        dado = arredondamento(dado);
        if(margem_outlier != 0) { // isto é, ainda não existe um valor atribuído por isso está no default
            verificar_outliers(dado, cultura);
        }
        verificar_lista_outliers();
        if(valido == 0) {
            if(sensorTipo == 't') {
                alertatemperatura(dado, cultura);
            }
            if(sensorTipo == 'l') {
                alertaLuz(dado, cultura);
            }
            if(sensorTipo == 'h') {
                alertaHumidade(dado, cultura);

            }
        }
    }

    public static double arredondamento(double medicao) {
        BigDecimal decimal = new BigDecimal(medicao).setScale(2, RoundingMode.HALF_EVEN);
        return decimal.doubleValue();
    }

    public int verificacao_validade(double medicao, Cultura cultura) {
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

    public void verificar_outliers(double medicao, Cultura cultura) {  // adicionar aqui o tipo de sensor ??
        if(verificacao_validade(medicao, cultura) == 0) {
            double diferenca = Math.abs(ultimo_valido - medicao);
            if(count_Outlier > 2) {
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
        System.out.println(valido);
        outliers.add(valido);
        outliers.remove(0);
    }


    public void verificar_lista_outliers() { // da para reduzir codigo se for apenas um if
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
        if(count_1 >= 2) {
            alertaoutlier();
        }
        else if(count_1 + count_2 >= 4) {
            alertaoutlier();
        }
    }


    private void alertaoutlier() {
        //envia para o mysql o alerta
        //limpar a lista e criar uma nova
        outliers.clear();
        criarlista();
        System.out.println("foi enviado um alerta outlier");

    }

    private void alertatemperatura(double medicao, Cultura cultura) throws InterruptedException { // estamos a enviar todos os avisos, mudar a ordem
        if(	cultura.getLimiteTempMax() < medicao || cultura.getLimiteTempMin() > medicao) {
            countV++;
            countL = 0;
            countA = 0;
            if(countV >= 2) {
                alertavermelho(cultura);
                countV = 0;
            }
        }
        else if((cultura.getAvisoTempMax() + cultura.getLimiteTempMax()) / 2 < medicao || (cultura.getAvisoTempMin() + cultura.getLimiteTempMin()) / 2 > medicao) {
            countL++;
            countV = 0;
            countA = 0;
            if(countL >= 2) {
                alertalaranja(cultura);
                countL = 0;
            }
        }
        else if(cultura.getAvisoTempMax() < medicao || cultura.getAvisoTempMin() > medicao) {

            countA++;
            countV = 0;
            countL = 0;
            if(countA >= 2) {
                alertaamarelo(cultura);
                countA = 0;
            }
        }
    }

    private void alertaHumidade(double medicao, Cultura cultura) throws InterruptedException { // estamos a enviar todos os avisos, mudar a ordem

        if( cultura.getLimiteHumMax() < medicao || cultura.getLimiteHumMin() > medicao) {
            countV++;
            countL = 0;
            countA = 0;
            if(countV >= 2) {
                alertavermelho(cultura);
                countV = 0;
            }
        }
        else if((cultura.getAvisoHumMax() + cultura.getLimiteHumMax()) / 2 < medicao || (cultura.getAvisoHumMin() + cultura.getLimiteHumMin()) / 2 > medicao) {
            countL++;
            countV = 0;
            countA = 0;
            if(countL >= 2) {
                alertalaranja(cultura);
                countL = 0;
            }
        }
        else if(cultura.getAvisoHumMax() < medicao || cultura.getAvisoHumMin() > medicao) {
            countA++;
            countV = 0;
            countL = 0;
            if(countA >= 2) {
                alertaamarelo(cultura);
                countA = 0;
            }
        }
    }

    private void alertaLuz(double medicao, Cultura cultura) throws InterruptedException { // estamos a enviar todos os avisos, mudar a ordem

        if( cultura.getLimiteLuzMax() < medicao || cultura.getLimiteLuzMin() > medicao) {
            countV++;
            countL = 0;
            countA = 0;
            if(countV >= 2) {
                alertavermelho(cultura);
                countV = 0;
            }
        }
        else if((cultura.getAvisoLuzMax() + cultura.getLimiteLuzMax()) / 2 < medicao || (cultura.getAvisoLuzMin() + cultura.getLimiteLuzMin()) / 2 > medicao) {
            countL++;
            countV = 0;
            countA = 0;
            if(countL >= 2) {
                alertalaranja(cultura);
                countL = 0;
            }
        }
        else if(cultura.getAvisoLuzMax() < medicao || cultura.getAvisoLuzMin() > medicao) {
            countA++;
            countV = 0;
            countL = 0;
            if(countA >= 2) {
                alertaamarelo(cultura);
                countA = 0;
            }
        }
    }

    Timestamp datAmarelo;
    Timestamp IntAmarelo;
    Timestamp datlaranja;
    Timestamp Intlaranja;
    Timestamp datvermelho;
    Timestamp Intvermelho;

    private void alertavermelho(Cultura cultura) throws InterruptedException {
        datAmarelo = null;
        datlaranja = null;
        if(datvermelho == null) {
            System.out.println("Datvermelho é nulo");
            datvermelho = new Timestamp(System.currentTimeMillis());
            Intvermelho = new Timestamp(System.currentTimeMillis() + (cultura.getIntervaloAviso() * 1000)); // vezes 60

            System.out.println("foi enviado um alerta vermelho entrei na zona 1");
            //envia para o mysql o alerta
            //String query = "INSERT INTO medicao (Zona, Sensor, Data, Valido, Leitura) VALUES ('" + zona + "','" + sensor + "','" + data + "','" + "1" + "','" + leitura + "');";
        }
        else {
            System.out.println("vai comparar as datas");
            datvermelho = new Timestamp(System.currentTimeMillis());
            System.out.println(datvermelho);
            System.out.println(Intvermelho);
            if(datvermelho.after(Intvermelho)) {
                //envia para o mysql o alerta

                System.out.println("foi enviado um alerta vermelho entrei na zona 2");
                Intvermelho = new Timestamp(System.currentTimeMillis() + (cultura.getIntervaloAviso() * 1000));
            }
        }
    }

    private void alertalaranja(Cultura cultura) throws InterruptedException {
        datAmarelo = null;
        datvermelho = null;
        if(datlaranja == null) {

            datlaranja = new Timestamp(System.currentTimeMillis());
            Intlaranja = new Timestamp(System.currentTimeMillis() + (cultura.getIntervaloAviso() * 1000)); // vezes 60
            //envia para o mysql o alerta
            System.out.println("foi enviado um alerta laranja entrei na zona 1");
        }
        else {

            datlaranja = new Timestamp(System.currentTimeMillis());

            if(datlaranja.after(Intlaranja)) {
                //envia para o mysql o alerta

                System.out.println("foi enviado um alerta laranja entrei na zona 2");
                Intlaranja = new Timestamp(System.currentTimeMillis() + (cultura.getIntervaloAviso() * 1000));
            }
        }
    }

    private void alertaamarelo(Cultura cultura) throws InterruptedException {
        datlaranja = null;
        datvermelho = null;
        if(datAmarelo == null) {
            datAmarelo = new Timestamp(System.currentTimeMillis());
            IntAmarelo = new Timestamp(System.currentTimeMillis() + (cultura.getIntervaloAviso() * 1000)); // vezes 60
            //envia para o mysql o alerta
            System.out.println("foi enviado um alerta amarelo entrei na zona 1");
        }
        else {
            datAmarelo = new Timestamp(System.currentTimeMillis());
            if(datAmarelo.after(IntAmarelo)) {
                //envia para o mysql o alerta
                System.out.println("foi enviado um alerta amarelo entrei na zona 2");
                IntAmarelo = new Timestamp(System.currentTimeMillis() + (cultura.getIntervaloAviso() * 1000));
            }
        }
    }


}

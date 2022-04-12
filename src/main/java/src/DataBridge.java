package src;



//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.util.JSON;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import org.bson.Document;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONException;
import org.json.JSONObject;

public class DataBridge implements MqttCallback {
    static String Origin = new String();
    static String Destination_Mongo = new String();
    static String Destination_Mysql = new String();
    static String Destination_Cloud = new String();
    static JTextArea documentLabel = new JTextArea("\n");
    static MqttClient mqttclient;
    static String cloud_server_to = new String();
    static String cloud_topic_to = new String();
    static String cloud_server_from = new String();
    static String cloud_topic_from = new String();
    static Connection connFrom;
    static String sql_database_connection_from = new String();
    static String sql_database_password_from = new String();
    static String sql_database_user_from = new String();
    static String sql_select_from = new String();
    static Connection connTo;
    static String sql_database_connection_to = new String();
    static String sql_database_password_to = new String();
    static String sql_database_user_to = new String();
    static String sql_table_to = new String();
    static MongoClient mongoClient_from;
    static String mongo_user_from = new String();
    static String mongo_password_from = new String();
    static String mongo_replica_from = new String();
    static String mongo_address_from = new String();
    static String mongo_database_from = new String();
    static String mongo_collection_from = new String();
    static String mongo_authentication_from = new String();
    static String create_backup = new String();
    static String backup_collection = new String();
    static String loop_query = new String();
    static String delete_document = new String();
    static String mongo_criteria = new String();
    static String mongo_fieldquery = new String();
    static String mongo_fieldvalue = new String();
    static String seconds_wait_mongo = new String();
    static MongoClient mongoClient_to;
    static String mongo_user_to = new String();
    static String mongo_password_to = new String();
    static String mongo_replica_to = new String();
    static String mongo_address_to = new String();
    static String mongo_database_to = new String();
    static String mongo_collection_to = new String();
    static String mongo_authentication_to = new String();
    static String zona = new String();
    static String sensor = new String();
    static String seconds_wait_sensor = new String();
    static String limiteInferior = new String();
    static String limiteSuperior = new String();
    static String valorInicial = new String();
    static String variacao = new String();
    static String medicoesIguais = new String();
    static String medicoesEntreSalto = new String();
    static String valorSalto = new String();
    static String medicoesSalto = new String();

    public DataBridge() {
    }

    private static void createWindow() {
        JFrame var0 = new JFrame("Data Migration");
        var0.setDefaultCloseOperation(3);
        JLabel var1 = new JLabel("Data : ", 0);
        var1.setPreferredSize(new Dimension(600, 30));
        JScrollPane var2 = new JScrollPane(documentLabel, 22, 32);
        var2.setPreferredSize(new Dimension(600, 200));
        JButton var3 = new JButton("Stop the program");
        var0.getContentPane().add(var1, "First");
        var0.getContentPane().add(var2, "Center");
        var0.getContentPane().add(var3, "Last");
        var0.setLocationRelativeTo((Component)null);
        var0.pack();
        var0.setVisible(true);
        var3.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent var1) {
                System.exit(0);
            }
        });
    }

    public static void main(String[] var0) {
        createWindow();

        try {
            Properties var1 = new Properties();
            var1.load(new FileInputStream("DataBridge.ini"));
            Destination_Mongo = var1.getProperty("Destination_Mongo");
            Destination_Mysql = var1.getProperty("Destination_Mysql");
            Destination_Cloud = var1.getProperty("Destination_Cloud");
            Origin = var1.getProperty("Origin");
            sql_select_from = var1.getProperty("sql_select_from");
            sql_database_connection_from = var1.getProperty("sql_database_connection_from");
            sql_database_password_from = var1.getProperty("sql_database_password_from");
            sql_database_user_from = var1.getProperty("sql_database_user_from");
            sql_table_to = var1.getProperty("sql_table_to");
            sql_database_connection_to = var1.getProperty("sql_database_connection_to");
            sql_database_password_to = var1.getProperty("sql_database_password_to");
            sql_database_user_to = var1.getProperty("sql_database_user_to");
            cloud_server_from = var1.getProperty("cloud_server_from");
            cloud_topic_from = var1.getProperty("cloud_topic_from");
            cloud_server_to = var1.getProperty("cloud_server_to");
            cloud_topic_to = var1.getProperty("cloud_topic_to");
            mongo_address_from = var1.getProperty("mongo_address_from");
            mongo_user_from = var1.getProperty("mongo_user_from");
            mongo_password_from = var1.getProperty("mongo_password_from");
            mongo_database_from = var1.getProperty("mongo_database_from");
            mongo_collection_from = var1.getProperty("mongo_collection_from");
            mongo_fieldquery = var1.getProperty("mongo_fieldquery");
            mongo_fieldvalue = var1.getProperty("mongo_fieldvalue");
            mongo_authentication_from = var1.getProperty("mongo_authentication_from");
            mongo_replica_from = var1.getProperty("mongo_replica_from");
            create_backup = var1.getProperty("create_backup");
            backup_collection = var1.getProperty("backup_collection");
            loop_query = var1.getProperty("loop_query");
            delete_document = var1.getProperty("delete_document");
            seconds_wait_mongo = var1.getProperty("delay");
            mongo_address_to = var1.getProperty("mongo_address_to");
            mongo_user_to = var1.getProperty("mongo_user_to");
            mongo_password_to = var1.getProperty("mongo_password_to");
            mongo_database_to = var1.getProperty("mongo_database_to");
            mongo_collection_to = var1.getProperty("mongo_collection_to");
            mongo_replica_to = var1.getProperty("mongo_replica_to");
            mongo_authentication_to = var1.getProperty("mongo_authentication_to");
            zona = var1.getProperty("Zona");
            sensor = var1.getProperty("Sensor");
            seconds_wait_sensor = var1.getProperty("seconds_wait_sensor");
            valorInicial = var1.getProperty("ValorInicial");
            limiteInferior = var1.getProperty("LimiteInferior");
            limiteSuperior = var1.getProperty("LimiteSuperior");
            variacao = var1.getProperty("Variacao");
            medicoesIguais = var1.getProperty("MedicoesIguais");
            medicoesEntreSalto = var1.getProperty("MedicoesEntreSalto");
            valorSalto = var1.getProperty("ValorSalto");
            medicoesSalto = var1.getProperty("MedicoesSalto");
        } catch (Exception var2) {
            System.out.println("Error reading DataMigration.ini file " + var2);
            JOptionPane.showMessageDialog((Component)null, "The DataMigration inifile wasn't found.", "Data Migration", 0);
        }

        if (Origin.equals("Mongo")) {
            (new DataBridge()).ConnectMongoFrom();
        }

        if (Origin.equals("Cloud")) {
            (new DataBridge()).ConnectCloudFrom();
        }

        if (Origin.equals("Mysql")) {
            (new DataBridge()).connectDatabase_from();
        }

        if (Destination_Mongo.equals("true")) {
            (new DataBridge()).ConnectMongoTo();
        }

        if (Destination_Cloud.equals("true")) {
            (new DataBridge()).ConnectCloudTo();
        }

        if (Destination_Mysql.equals("true")) {
            (new DataBridge()).connectDatabase_to();
        }

        if (Origin.equals("Mongo")) {
            (new DataBridge()).ReadMongoFrom();
        }

        if (Origin.equals("Mysql")) {
            (new DataBridge()).SelectDataFromMysql();
        }

        if (Origin.equals("Sensor")) {
            (new DataBridge()).generateData();
        }

    }

    public void ConnectCloudFrom() {
        try {
            String var10002 = cloud_server_from;
            String var10003 = this.getSaltString();
            mqttclient = new MqttClient(var10002, "DataMigration" + var10003 + cloud_topic_from);
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_topic_from);
            documentLabel.append("Cloud Server: " + cloud_server_from + "\n");
            documentLabel.append("Cloud Topic: " + cloud_topic_from + "\n");
            documentLabel.append("Connection To Cloud From Suceeded\n");
        } catch (MqttException var2) {
            var2.printStackTrace();
        }

    }

    public void ConnectCloudTo() {
        try {
            String var10002 = cloud_server_to;
            String var10003 = this.getSaltString();
            mqttclient = new MqttClient(var10002, "DataMigration" + var10003 + cloud_topic_to);
            mqttclient.connect();
            mqttclient.setCallback(this);
            mqttclient.subscribe(cloud_topic_to);
            documentLabel.append("Cloud Server: " + cloud_server_to + "\n");
            documentLabel.append("Cloud Topic: " + cloud_topic_to + "\n");
            documentLabel.append("Connection To Cloud (To " + cloud_topic_to + ") Suceeded\n");
        } catch (MqttException var2) {
            var2.printStackTrace();
        }

    }

    public void ConnectMongoFrom() {
        new String();
        String var1 = "mongodb://";
        if (mongo_authentication_from.equals("true")) {
            var1 = var1 + mongo_user_from + ":" + mongo_password_from + "@";
        }

        var1 = var1 + mongo_address_from;
        if (!mongo_replica_from.equals("false")) {
            if (mongo_authentication_from.equals("true")) {
                var1 = var1 + "/?replicaSet=" + mongo_replica_from + "&authSource=admin";
            } else {
                var1 = var1 + "/?replicaSet=" + mongo_replica_from;
            }
        } else if (mongo_authentication_from.equals("true")) {
            var1 = var1 + "/?authSource=admin";
        }

        mongoClient_from = new MongoClient(new MongoClientURI(var1));
        documentLabel.append("Mongo Server: " + var1 + "\n");
        documentLabel.append("Connection To Mongo Origin Suceeded\n");
    }

    public void ConnectMongoTo() {
        new String();
        String var1 = "mongodb://";
        if (mongo_authentication_to.equals("true")) {
            var1 = var1 + mongo_user_to + ":" + mongo_password_to + "@";
        }

        var1 = var1 + mongo_address_to;
        if (!mongo_replica_to.equals("false")) {
            if (mongo_authentication_to.equals("true")) {
                var1 = var1 + "/?replicaSet=" + mongo_replica_to + "&authSource=admin";
            } else {
                var1 = var1 + "/?replicaSet=" + mongo_replica_to;
            }
        } else if (mongo_authentication_to.equals("true")) {
            var1 = var1 + "/?authSource=admin";
        }

        mongoClient_to = new MongoClient(new MongoClientURI(var1));
        documentLabel.append("Mongo Server: " + var1 + "\n");
        documentLabel.append("Connection To Mongo Destination Suceeded\n");
    }

    public void connectDatabase_from() {
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            connFrom = DriverManager.getConnection(sql_database_connection_from, sql_database_user_from, sql_database_password_from);
            documentLabel.append("SQl Connection:" + sql_database_connection_from + "\n");
            documentLabel.append("Connection To MariaDB From Suceeded\n");
        } catch (Exception var2) {
            System.out.println("Mysql Server Origin down, unable to make the connection. " + var2);
        }

    }

    public void connectDatabase_to() {
        try {
            Class.forName("org.mariadb.jdbc.Driver");
            connTo = DriverManager.getConnection(sql_database_connection_to, sql_database_user_to, sql_database_password_to);
            documentLabel.append("SQl Connection:" + sql_database_connection_to + "\n");
            documentLabel.append("Connection To MariaDB To Suceeded\n");
        } catch (Exception var2) {
            System.out.println("Mysql Server Destination down, unable to make the connection. " + var2);
        }

    }

    public void ReadMongoFrom() {
        new String();
        MongoDatabase var2 = mongoClient_from.getDatabase(mongo_database_from);
        MongoCollection var3 = var2.getCollection(mongo_collection_from);
        MongoCollection var4 = var2.getCollection(backup_collection);
        Document var5 = new Document();
        if (!mongo_fieldquery.equals("null")) {
            var5.put(mongo_fieldquery, mongo_fieldvalue);
        }

        boolean var6 = false;

        while(!var6) {
            FindIterable var7 = var3.find(var5);
            MongoCursor var8 = var7.iterator();
            MongoCursor var9 = var7.projection(Projections.excludeId()).iterator();
            int var10 = 1;

            while(var9.hasNext()) {
                new Document();
                Document var11 = (Document)var9.next();

                try {
                    documentLabel.getDocument().insertString(0, var11.toString() + "\n", (AttributeSet)null);
                } catch (Exception var14) {
                }

                if (Destination_Mongo.equals("true")) {
                    this.WriteMongoTo(var11);
                }

                if (Destination_Cloud.equals("true")) {
                    this.WriteSensorTo(JSON.serialize(var11));
                }

                if (Destination_Mysql.equals("true")) {
                    this.WriteMySQL(JSON.serialize(var11), "json");
                }

                ++var10;
                if (create_backup.equals("true")) {
                    var4.insertOne(var11);
                }

                if (!seconds_wait_mongo.equals("0")) {
                    try {
                        Thread.sleep((long)Integer.parseInt(seconds_wait_mongo));
                    } catch (Exception var13) {
                    }
                }
            }

            if (delete_document.equals("true")) {
                if (!mongo_fieldquery.equals("null")) {
                    var3.deleteMany(Filters.eq(mongo_fieldquery, mongo_fieldvalue));
                }

                if (mongo_fieldquery.equals("null")) {
                    var2.getCollection(mongo_collection_from).drop();
                }
            }

            if (!loop_query.equals("true")) {
                var6 = true;
            }
        }

    }

    public void messageArrived(String var1, MqttMessage var2) throws Exception {
        try {
            if (Origin.equals("Cloud")) {
                if (Destination_Mongo.equals("true")) {
                    DBObject var3 = (DBObject)JSON.parse(var2.toString());
                    this.WriteMongoTo(new Document(var3.toMap()));
                }

                if (Destination_Cloud.equals("true")) {
                    this.WriteSensorTo(var2.toString());
                }

                if (Destination_Mysql.equals("true")) {
                    this.WriteMySQL(var2.toString(), "nojson");
                }

                try {
                    documentLabel.getDocument().insertString(0, var2.toString() + "\n", (AttributeSet)null);
                } catch (Exception var4) {
                }
            }
        } catch (Exception var5) {
            System.out.println(var5);
        }

    }

    public void SelectDataFromMysql() {
        new String();
        new String();
        new ArrayList();

        try {
            Statement var4 = connFrom.createStatement();
            ResultSet var5 = var4.executeQuery(sql_select_from);
            ResultSetMetaData var6 = var5.getMetaData();

            while(var5.next()) {
                int var7 = var6.getColumnCount();
                JSONObject var8 = new JSONObject();

                for(int var9 = 1; var9 < var7 + 1; ++var9) {
                    String var10 = var6.getColumnName(var9);

                    try {
                        if (var6.getColumnType(var9) == 2003) {
                            var8.put(var10, var5.getArray(var10));
                        } else if (var6.getColumnType(var9) == -5) {
                            var8.put(var10, var5.getInt(var10));
                        } else if (var6.getColumnType(var9) == 16) {
                            var8.put(var10, var5.getBoolean(var10));
                        } else if (var6.getColumnType(var9) == 2004) {
                            var8.put(var10, var5.getBlob(var10));
                        } else if (var6.getColumnType(var9) == 8) {
                            var8.put(var10, var5.getDouble(var10));
                        } else if (var6.getColumnType(var9) == 6) {
                            var8.put(var10, (double)var5.getFloat(var10));
                        } else if (var6.getColumnType(var9) == 4) {
                            var8.put(var10, var5.getInt(var10));
                        } else if (var6.getColumnType(var9) == -9) {
                            var8.put(var10, var5.getNString(var10));
                        } else if (var6.getColumnType(var9) == 12) {
                            var8.put(var10, var5.getString(var10));
                        } else if (var6.getColumnType(var9) == -6) {
                            var8.put(var10, var5.getInt(var10));
                        } else if (var6.getColumnType(var9) == 5) {
                            var8.put(var10, var5.getInt(var10));
                        } else if (var6.getColumnType(var9) == 91) {
                            var8.put(var10, var5.getDate(var10));
                        } else if (var6.getColumnType(var9) == 93) {
                            var8.put(var10, var5.getTimestamp(var10));
                        } else {
                            var8.put(var10, var5.getObject(var10));
                        }
                    } catch (JSONException var13) {
                        var13.printStackTrace();
                    }
                }

                Document var15 = Document.parse(var8.toString());

                try {
                    documentLabel.getDocument().insertString(0, var8.toString() + "\n", (AttributeSet)null);
                } catch (Exception var12) {
                }

                if (Destination_Mongo.equals("true")) {
                    this.WriteMongoTo(var15);
                }

                if (Destination_Cloud.equals("true")) {
                    this.WriteSensorTo(var8.toString());
                }

                if (Destination_Mysql.equals("true")) {
                    this.WriteMySQL(var8.toString(), "nojson");
                }
            }
        } catch (SQLException var14) {
            var14.printStackTrace();
        }

    }

    public void generateData() {
        double var1 = 0.0D;
        double var3 = Double.parseDouble(variacao);
        double var5 = Double.parseDouble(medicoesEntreSalto);
        double var7 = Double.parseDouble(limiteSuperior);
        double var9 = Double.parseDouble(limiteInferior);
        SimpleDateFormat var11 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        new String();
        boolean var14 = false;
        boolean var15 = false;
        int var16 = 0;
        double var17 = 0.0D;
        int var19 = 0;
        byte var20 = 1;
        var1 = Double.parseDouble(valorInicial);
        new Date(System.currentTimeMillis());

        while(true) {
            String var12;
            Date var21;
            DBObject var22;
            do {
                var21 = new Date(System.currentTimeMillis());
                var12 = "{";
                var12 = var12 + "Zona: \"" + zona + "\", ";
                var12 = var12 + "Sensor: \"" + sensor + "\", ";
                var12 = var12 + "Data: \"" + var11.format(var21) + "\", ";
                var12 = var12 + "Medicao: \"" + var1 + "\" ";
                var12 = var12 + "}";

                try {
                    documentLabel.getDocument().insertString(0, var12 + "\n", (AttributeSet)null);
                } catch (BadLocationException var27) {
                }

                var22 = (DBObject)JSON.parse(var12);
                if (Destination_Mongo.equals("true")) {
                    this.WriteMongoTo(new Document(var22.toMap()));
                }

                if (Destination_Cloud.equals("true")) {
                    this.WriteSensorTo(var12);
                }

                if (Destination_Mysql.equals("true")) {
                    this.WriteMySQL(var12, "nojson");
                }

                if (!seconds_wait_sensor.equals("0")) {
                    try {
                        Thread.sleep((long)Integer.parseInt(seconds_wait_sensor));
                    } catch (Exception var24) {
                    }
                }

                if (var20 == 1 && var1 > var7) {
                    var20 = 4;
                }

                if (var20 == 0 && var1 < var9) {
                    var20 = 3;
                }

                if (var20 > 2) {
                    ++var16;
                }

                if (var20 == 4 && var16 > Integer.parseInt(medicoesIguais)) {
                    var20 = 0;
                    var16 = 0;
                }

                if (var20 == 3 && var16 > Integer.parseInt(medicoesIguais)) {
                    var20 = 1;
                    var16 = 0;
                }

                if (var20 == 1) {
                    var1 += var3;
                }

                if (var20 == 0) {
                    var1 -= var3;
                }

                ++var19;
            } while(!((double)var19 > var5));

            var19 = 0;
            var17 = var1 + Double.parseDouble(valorSalto);

            for(int var13 = 0; var13 < Integer.parseInt(medicoesSalto); ++var13) {
                var21 = new Date(System.currentTimeMillis());
                var12 = "{";
                var12 = var12 + "Zona: \"" + zona + "\", ";
                var12 = var12 + "Sensor: \"" + sensor + "\", ";
                var12 = var12 + "Data: \"" + var11.format(var21) + "\", ";
                var12 = var12 + "Medicao: \"" + var17 + "\" ";
                var12 = var12 + "}";

                try {
                    documentLabel.getDocument().insertString(0, var12 + "\n", (AttributeSet)null);
                } catch (BadLocationException var26) {
                }

                var22 = (DBObject)JSON.parse(var12);
                if (Destination_Mongo.equals("true")) {
                    this.WriteMongoTo(new Document(var22.toMap()));
                }

                if (Destination_Cloud.equals("true")) {
                    this.WriteSensorTo(var12);
                }

                if (Destination_Mysql.equals("true")) {
                    this.WriteMySQL(var12, "nojson");
                }

                if (!seconds_wait_sensor.equals("0")) {
                    try {
                        Thread.sleep((long)Integer.parseInt(seconds_wait_sensor));
                    } catch (Exception var25) {
                    }
                }
            }
        }
    }

    private void WriteMongoTo(Document var1) {
        DB var3 = mongoClient_to.getDB(mongo_database_to);
        DBCollection var2 = var3.getCollection(mongo_collection_to);
        new Document();
        new String();
        String var5 = var1.toJson();

        try {
            DBObject var6 = (DBObject)JSON.parse(var5);
            var2.insert(new DBObject[]{var6});
        } catch (Exception var8) {
            System.out.println(var8);
        }

        if (!seconds_wait_mongo.equals("0")) {
            try {
                Thread.sleep((long)Integer.parseInt(seconds_wait_mongo));
            } catch (Exception var7) {
            }
        }

    }

    private void WriteMongoTo2(DBObject var1) {
        System.out.println("****");
        System.out.println(var1);
        DB var3 = mongoClient_to.getDB(mongo_database_to);
        DBCollection var2 = var3.getCollection(mongo_collection_to);

        try {
            var2.insert(new DBObject[]{var1});
        } catch (Exception var6) {
            System.out.println(var6);
        }

        if (!seconds_wait_mongo.equals("0")) {
            try {
                Thread.sleep((long)Integer.parseInt(seconds_wait_mongo));
            } catch (Exception var5) {
            }
        }

    }

    public void WriteSensorTo(String var1) {
        try {
            MqttMessage var2 = new MqttMessage();
            var2.setPayload(var1.getBytes());
            mqttclient.publish(cloud_topic_to, var2);
        } catch (MqttException var3) {
            var3.printStackTrace();
        }

    }

    public void WriteMySQL(String var1, String var2) {
        Pattern var3 = Pattern.compile("\"([^{]*?)\"\\s:\\s\"(.*?)\"");
        Matcher var4 = var3.matcher(var1);
        new String();
        new String();
        String var5 = "{";
        String var10000;
        if (!var2.equals("json")) {
            var5 = var1;
        } else {
            while(true) {
                if (!var4.find()) {
                    var5 = var5.substring(0, var5.length() - 1);
                    var5 = var5 + "}";
                    break;
                }

                var10000 = var4.group(1);
                String var6 = var10000 + ": \"" + var4.group(2);
                var6 = var6.replace(',', '.');
                var5 = var5 + var6 + "\",";
            }
        }

        new String();
        new String();
        new String();
        new String();
        String var7 = "";
        String var8 = "";
        String var10 = " ";
        String var11 = var5.toString();
        String[] var12 = var11.split(",");

        for(int var13 = 0; var13 < var12.length; ++var13) {
            String[] var14 = var12[var13].split(":");
            if (var13 == 0) {
                var7 = var14[0];
            } else {
                var7 = var7 + ", " + var14[0];
            }

            if (var13 == 0) {
                var8 = var14[1];
            } else {
                var8 = var8 + ", " + var14[1];
            }
        }

        var7 = var7.replace("\"", "");
        var10000 = sql_table_to;
        String var9 = "Insert into " + var10000 + " (" + var7.substring(1, var7.length()) + ") values (" + var8.substring(0, var8.length() - 1) + ");";

        try {
            Statement var16 = connTo.createStatement();
            int var17 = new Integer(var16.executeUpdate(var9));
            var16.close();
        } catch (Exception var15) {
            System.out.println("Error Inserting in the database . " + var15);
        }

    }

    public void connectionLost(Throwable var1) {
    }

    public void deliveryComplete(IMqttDeliveryToken var1) {
    }

    protected String getSaltString() {
        String var1 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder var2 = new StringBuilder();
        Random var3 = new Random();

        while(var2.length() < 18) {
            int var4 = (int)(var3.nextFloat() * (float)var1.length());
            var2.append(var1.charAt(var4));
        }

        String var5 = var2.toString();
        return var5;
    }
}

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class MongoToMQTT {

    private static String urllocal = "mongodb://localhost:27019,localhost:25019,localhost:23019/?replicaSet=medicoespisid";

    private static String databaselocal = "medicoes";

    //PERGUNTAR OU VER SE PODEMOS USAR ESTE QUE É O DO PROF Fábio
    private String cloudServer = "tcp://broker.mqtt-dashboard.com:1883";

    private String topicot1 = "G22T1";
    private String topicot2 = "G22T1";
    private String topicoh1 = "G22H1";
    private String topicoh2 = "G22H2";
    private String topicol1 = "G22L1";
    private String topicol2 = "G22L2";

    //locais
    private static String collectiont1 = "t1";
    private static String collectiont2 = "t2";
    private static String collectionh1 = "h1";
    private static String collectionh2 = "h2";
    private static String collectionl1 = "l1";
    private static String collectionl2 = "l2";

    private static String datat1;
    private static String datat2;
    private static String datal1;
    private static String datal2;
    private static String datah1;
    private static String datah2;

    //auxiliares de verificação de vazios iniciais
    private static Boolean t1vazia=false;
    private static Boolean t2vazia=false;
    private static Boolean h1vazia=false;
    private static Boolean h2vazia=false;
    private static Boolean l1vazia=false;
    private static Boolean l2vazia=false;

    //coleções locais
    private static MongoCollection<Document> localcollectiont1;
    private static MongoCollection<Document> localcollectiont2;
    private static MongoCollection<Document> localcollectionh1;
    private static MongoCollection<Document> localcollectionh2;
    private static MongoCollection<Document> localcollectionl1;
    private static MongoCollection<Document> localcollectionl2;

    private static MongoClient localMongoClient;

    private static MongoDatabase localMongoDatabase;

    public static void main(String args[]) throws InterruptedException {

        //verificar se já existem dados no mysql

        //se não existirem enviar 1 doc de cada sensor

        while(true){
            //obter doc mais recente de cada sensor comparar com a data em memória

            //converter medição de cada sensor em string

            //enviar para o topico

            //guardar data de cada medicao enviada na vairiavel data
        }

    }

}

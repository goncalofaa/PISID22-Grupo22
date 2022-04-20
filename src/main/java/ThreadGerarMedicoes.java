import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class ThreadGerarMedicoes extends Thread{
	private String zona;
	private String sensor;
	private MongoCollection<Document> cloudCollectionDoc;
	
    public ThreadGerarMedicoes(String zona, String sensor, MongoDatabase cloudMongoDatabase ,String cloudCollectionDoc) {
		this.zona = zona;
		this.sensor = sensor;
		this.cloudCollectionDoc = cloudMongoDatabase.getCollection(cloudCollectionDoc);
	}

	@Override
    public void run() {
        //noinspection InfiniteLoopStatement
        while (true) {
        	//System.out.println("A gerar novos dados na nuvem: zona " + zona + " Sensor " + sensor);
        	gerarDadosNuvem();
        }
    } 
	public void gerarDadosNuvem() {
		
		for (int i = 0; i < 10; i++) {
			
			Random rnd = new Random();
			double temp =  10 + rnd.nextDouble() * 15.0;
			String tempString = String.format("%04.1f",temp);
			tempString = tempString.replace(",", ".");
			
			SimpleDateFormat formatter= new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
			Date date = new Date(System.currentTimeMillis());
			Document document = new Document();
		    document.append("Zona", zona);
		    document.append("Sensor", sensor);
		    document.append("Data", formatter.format(date));
		    document.append("Medicao", tempString);
		    
		    cloudCollectionDoc.insertOne(document);
		    try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
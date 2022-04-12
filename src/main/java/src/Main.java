package src;

public class Main {

    public static void main(String[] args){
        //Só fica a ultima thread a correr
        //Mas se correrem em mains separadas funcionam
        DataGenerator db2= new DataGenerator("Zona2","SensorT2","22t2");
        Thread t2 = new Thread(db2);
        t2.start();
        /*DataGenerator db= new DataGenerator("Zona1","SensorT1","22t1");
        Thread t = new Thread(db);
        t.start();*/
        /*DataGenerator db2= new DataGenerator("Zona2","SensorT2","22t2");
        Thread t2 = new Thread(db2);
        t2.start();
        DataGenerator db3= new DataGenerator("Zona1","SensorL1","22l1");
        Thread t3 = new Thread(db3);
        t3.start();
        DataGenerator db4= new DataGenerator("Zona2","SensorL2","22l2");
        Thread t4 = new Thread(db4);
        t4.start();
        DataGenerator db5= new DataGenerator("Zona1","SensorH1","22h1");
        Thread t5 = new Thread(db5);
        t5.start();
        DataGenerator db6= new DataGenerator("Zona2","SensorH2","22h2");
        Thread t6 = new Thread(db6);
        t6.start();*/


    }


}

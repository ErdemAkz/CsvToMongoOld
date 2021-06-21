package com.converter;

import com.mongodb.MongoClient;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple7;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.util.List;

public class csvToMongo {
    public static void main(String[] args){

        try {

            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


            DataSet<Tuple7<Integer,String, String, String, Integer, Double, String>> data

                    = env.readCsvFile("src/main/resources/sales_orders.csv")
                    .ignoreFirstLine()
                    .parseQuotedStrings('\"')
                    .types(Integer.class, String.class, String.class, String.class,
                            Integer.class, Double.class,String.class);


            //rawOrders.print();


            MongoClient mongo = new MongoClient( "localhost" , 27017 );

            MongoDatabase database = mongo.getDatabase("erdemdb");

            database.getCollection("sampleCollection1").drop();
            database.getCollection("sampleCollection1");

            System.out.println("Collection created successfully");
            long start = System.currentTimeMillis();
            List<Tuple7<Integer,String, String, String, Integer, Double, String>> list = data.collect();
            int size = list.size();

            for (int i=0; i<size; i++) {
                //System.out.println(list.get(i));
                Document document = new Document();
                document.append("ID",list.get(i).f0);
                document.append("Customer",list.get(i).f1);
                document.append("Product",list.get(i).f2);
                document.append("Date",list.get(i).f3);
                document.append("Quantity",list.get(i).f4);
                document.append("Rate",list.get(i).f5);
                document.append("Tags",list.get(i).f6);
                database.getCollection("sampleCollection1").insertOne(document);

            }

            long stop = System.currentTimeMillis();
            System.out.println("Items has been successfully added to the collection");
            System.out.println(stop-start);
        }catch (Exception e){
            e.printStackTrace();
        }



    }

}

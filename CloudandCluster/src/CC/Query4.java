package CC;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.bson.Document;

public class Query4 {
    public static void main(String[] args) {

        MongoClient client = new MongoClient("localhost", 27017);
        MongoCredential credential;
        credential = MongoCredential.createCredential("sukanya", "freeway","mongo@123".toCharArray());
        MongoDatabase database = client.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("metadata");
        FindIterable<Document> metadata = collection.find();

        double stationLength = 0;
        ArrayList<String> detector_id = new ArrayList<String>();
        for (Document h : metadata) {
            if (String.valueOf(h.get("highwayname")).equalsIgnoreCase("Foster NB"))
            {
                ArrayList<Document> stations_arr = (ArrayList<Document>) metadata.get("stations");
                for (Document s : stations_arr) {
                    stationLength = stationLength +  Double.parseDouble((String) s.get("length"));
                    ArrayList<Document> detectors_arr = (ArrayList<Document>) s.get("detectors");
                    for (Document d : detectors_arr) {
                        detector_id.add(String.valueOf(d.get("detectorid")));
                    }
                }
            }
        }
        Set<Integer> ids = new HashSet<Integer>();
        for (String s : detector_id) {
            ids.add(Integer.parseInt(s));
        }

        MongoCollection<Document> loopdata = database.getCollection("loopdata");
        int sum = 0;
        Date startdateintial,enddateintial,startdatefinal,enddatefinal = null;

        startdateintial = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 07:00:00");
        enddateintial = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 09:00:00");
        startdatefinal = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 16:00:00");
        enddatefinal = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2011-09-22 18:00:00");

        FindIterable<Document> loop_data1 = loopdata.find(Filters.and(Filters.in("detectorid", ids),
                Filters.gte("starttime", startdateintial.getTime()), Filters.lte("starttime", enddateintial.getTime())));
        FindIterable<Document> loop_data2 = loopdata.find(Filters.and(Filters.in("detectorid", ids),
                Filters.gte("starttime", startdatefinal.getTime()), Filters.lte("starttime", enddatefinal.getTime())));
        long count = 0;
        for (Document loop : loop_data1) {
            if (loop.get("speed") != null) {
                if ((Integer) loop.get("volume") > 0) {
                    sum += (Integer) loop.get("speed");
                    count++;
                }
            }
        }
        for (Document loop : loop_data2) {
            if (loop.get("speed") != null) {
                if ((Integer) loop.get("volume") > 0) {
                    sum += (Integer) loop.get("speed");
                    count++;
                }
            }
        }

        double average = (double) sum / count;

        System.out.println("Find the average travel time for 7-9AM and 4-6PM on September 22, 2011 for station Foster NB in seconds: "+ (stationLength/average) * 3600);
        client.close();
    }
}

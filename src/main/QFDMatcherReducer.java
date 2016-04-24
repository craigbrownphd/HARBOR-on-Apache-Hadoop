import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;
import java.lang.Math.*;
import java.util.*;

public class QFDMatcherReducer extends Reducer<IntWritable, WebTrafficRecord, RequestReplyMatch, NullWritable> {

    ArrayList<WebTrafficRecord> recordArray = new ArrayList<>();

    @Override
    public void reduce(IntWritable key, Iterable<WebTrafficRecord> values,
                       Context ctxt) throws IOException, InterruptedException {

        // The input is a set of WebTrafficRecords for each key,
        // the output should be the WebTrafficRecords for
        // all cases where the request and reply are matched
        // as having the same
        // Source IP/Source Port/Destination IP/Destination Port
        // and have occured within a 10 second window on the timestamp.

        // One thing to really remember, the Iterable element passed
        // from hadoop are designed as READ ONCE data, you will
        // probably want to copy that to some other data structure if
        // you want to iterate mutliple times over the data.

        for (WebTrafficRecord entry : values) recordArray.add(new WebTrafficRecord(entry));
        
        for (WebTrafficRecord request : recordArray) {
            if (request.getUserName() == null) {
                for (WebTrafficRecord reply : recordArray) {
                    if (reply.getCookie() == null && request.tupleMatches(reply) && Math.abs(request.getTimestamp() - reply.getTimestamp()) <= 10 ) {
                        ctxt.write(new RequestReplyMatch(request, reply), NullWritable.get());
                    }
                }
            }
        }
    }
}

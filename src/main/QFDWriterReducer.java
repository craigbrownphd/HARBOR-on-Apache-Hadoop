import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ObjectOutputStream;
import java.io.IOException;
import java.util.*;

public class QFDWriterReducer extends Reducer<WTRKey, RequestReplyMatch, NullWritable, NullWritable> {

    @Override
    public void reduce(WTRKey key, Iterable<RequestReplyMatch> values,
                       Context ctxt) throws IOException, InterruptedException {

        // The input will be a WTR key and a set of matches.

        // You will want to open the file named
        // "qfds/key.getName()/key.getName()_key.getHashBytes()"
        // using the FileSystem interface for Hadoop.

        // EG, if the key's name is srcIP and the hash is 2BBB,
        // the filename should be qfds/srcIP/srcIP_2BBB

        // Some useful functionality:

        // FileSystem.get(ctxt.getConfiguration())
        // gets the interface to the filesysstem
        // new Path(filename) gives a path specification
        // hdfs.create(path, true) will create an
        // output stream pointing to that file
        Set<RequestReplyMatch> qfdSet = new HashSet<>();
        for (RequestReplyMatch match : values) qfdSet.add(match);
        QueryFocusedDataSet object = new QueryFocusedDataSet(key.getName(), key.getHashBytes(), qfdSet);

        FileSystem filesystem = FileSystem.get(ctxt.getConfiguration());
        Path url = new Path("qfds/" + key.getName() + "/" + key.getName() + "_" + key.getHashBytes());
        FSDataOutputStream outStream = filesystem.create(url, true);
        ObjectOutputStream objOutStream = new ObjectOutputStream(outStream);
        objOutStream.writeObject(object);
        outStream.close();

    }
}

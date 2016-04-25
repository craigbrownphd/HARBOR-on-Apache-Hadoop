import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import javax.xml.bind.DatatypeConverter;

public class TotalFailMapper extends Mapper<LongWritable, Text, WTRKey,
                                            RequestReplyMatch> {

    private MessageDigest messageDigest;
    @Override
    public void setup(Context ctxt) throws IOException, InterruptedException {
        super.setup(ctxt);
        try {
            messageDigest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            // SHA-1 is required on all Java platforms, so this
            // should never occur
            throw new RuntimeException("SHA-1 algorithm not available");
        }
        // Now we are adding the salt...
        messageDigest.update(HashUtils.SALT.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void map(LongWritable lineNo, Text line, Context ctxt)
            throws IOException, InterruptedException {

        // The value in the input for the key/value pair is a Tor IP.
        // You need to then query that IP's source QFD to get
        // all cookies from that IP,
        // query the cookie QFDs to get all associated requests
        // which are by those cookies, and store them in a torusers QFD

        // Get the WTRKey and hash from SrcIp
        MessageDigest md = HashUtils.cloneMessageDigest(messageDigest);
        md.update(line.toString().getBytes(StandardCharsets.UTF_8));
        byte[] hash = md.digest();
        byte[] hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
        String hashString = DatatypeConverter.printHexBinary(hashBytes);
        WTRKey key = new WTRKey("srcIP", hashString);

        // Load the file corresponding to that IP
        FileSystem filesystem = FileSystem.get(ctxt.getConfiguration());
        Path url = new Path("qfds/srcIP/srcIP_" + hashString);
        FSDataInputStream inStream = filesystem.open(url);
        ObjectInputStream objInStream = new ObjectInputStream(inStream);

        try{
            // QFDset retrieved.
            QueryFocusedDataSet object = (QueryFocusedDataSet)objInStream.readObject();
        }
        catch (ClassNotFoundException e) {
        }
        inStream.close();

        // Obtain all cookies from the QFDset.
        ArrayList<String> cookies = new ArrayList<>();
        for (RequestReplyMatch pair : object.getMatches()) cookies.add(pair.getCookie());
        





        //ctxt.write(key, record);

        System.err.println("You need to put some code here!");
    }
}

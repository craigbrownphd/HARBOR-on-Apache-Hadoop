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

        // From the loaded file (srcIP QFE), for all RequestReplyMatch, laod all cookies.
        QueryFocusedDataSet srcIP_QFD_object;
        ArrayList<String> cookies = new ArrayList<>();  // All cookies stored here!
        try{
            // QFDset retrieved.
            srcIP_QFD_object = (QueryFocusedDataSet)objInStream.readObject();
            // Obtain all cookies from the QFDset.
            for (RequestReplyMatch pair : srcIP_QFD_object.getMatches()) cookies.add(pair.getCookie());
        }
        catch (ClassNotFoundException e) {
        }
        inStream.close();

        // For each cookie we found above, open the coresponding file qfds/cookie/cookie_hash
        // then load all the RequestReplyMatch and emmit them.
        for (String cookie : cookies) {

            // Find the hash of the cookie
            md = HashUtils.cloneMessageDigest(messageDigest);
            md.update(cookie.getBytes(StandardCharsets.UTF_8));
            hash = md.digest();
            hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
            hashString = DatatypeConverter.printHexBinary(hashBytes);    // Cookie hash here.

            // Load the file corresponding to each cookie
            filesystem = FileSystem.get(ctxt.getConfiguration());
            url = new Path("qfds/cookie/cookie_" + hashString);
            inStream = filesystem.open(url);
            objInStream = new ObjectInputStream(inStream);

            // For each cookie file (QFD) load all RequestReplyMatch and emmit them.
            try{
                // QFDset retrieved.
                QueryFocusedDataSet object = (QueryFocusedDataSet)objInStream.readObject();
                for (RequestReplyMatch pair : object.getMatches()) {
                    // Hash the username for the key.
                    md = HashUtils.cloneMessageDigest(messageDigest);
                    md.update(pair.getUserName().getBytes(StandardCharsets.UTF_8));
                    hash = md.digest();
                    hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
                    hashString = DatatypeConverter.printHexBinary(hashBytes);
                    key = new WTRKey("torusers", hashString);
                    ctxt.write(key, pair);
                }
            }
            catch (ClassNotFoundException e) {
            }
            inStream.close();
            filesystem.closeAll();
        }
    }
}

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import javax.xml.bind.DatatypeConverter;

public class QFDWriterMapper extends Mapper<RequestReplyMatch,
				     NullWritable, WTRKey,
				     RequestReplyMatch> {
    private MessageDigest messageDigest;


    // For the Query Focused Dataset, we take the key we are using,
    // hash it with a cryptographic hash function, and only use
    // the HashUtils.NUM_HASH_BYTES.

    // We start by creating a messageDigest instance, and preseed it
    // with the salt also contained in HashUtils.  This is so that
    // nobody can predict which names map to which locations
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
	public void map(RequestReplyMatch record, NullWritable ignore, Context ctxt)
	        throws IOException, InterruptedException {

        // This is the key mapping function.  You should
        // have the outputs be WTRKey/RequestReplyMatch pairs
        // since this dictates how to go to a particular
        // QFD

        // As an example, to dispatch for the "srcIP", the
        // hash should be

            // MessageDigest md = HashUtils.cloneMessageDigest(messageDigest);
            // md.update(record.getSrcIp().getBytes(StandardCharsets.UTF_8));
            // byte[] hash = md.digest();
            // byte[] hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
            // String hashString = DatatypeConverter.printHexBinary(hashBytes);
            // WTRKey key = new WTRKey("srcIP", hashString);
            // ctxt.write(key, record);

        // You need to do srcIP, destIP, and cookie QFD dispatch, and should
        // be able to use a much more generic structure

        String[] get_srcIP_destIP_cookie = {record.getSrcIp(), record.getDestIp(), record.getCookie()};
        String[] srcIP_destIP_cookie = {"srcIP", "destIP", "cookie"};

        for (int i = 0; i < 3; i++) {
            MessageDigest md = HashUtils.cloneMessageDigest(messageDigest);
            md.update(get_srcIP_destIP_cookie[i].getBytes(StandardCharsets.UTF_8));
            byte[] hash = md.digest();
            byte[] hashBytes = Arrays.copyOf(hash, HashUtils.NUM_HASH_BYTES);
            String hashString = DatatypeConverter.printHexBinary(hashBytes);
            WTRKey key = new WTRKey(srcIP_destIP_cookie[i], hashString);
            ctxt.write(key, record);
        }
    }
}

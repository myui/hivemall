package hivemall.utils.hashing;

import java.util.Random;

import org.junit.Test;
import org.junit.Assert;

public class MurmurHash3Test {

    @Test
    public void testMurmurhash3String() {
        for(int i = 0; i < 100; i++) {
            Random rand = new Random();
            int v = rand.nextInt(Integer.MAX_VALUE);
            String s = Integer.toOctalString(v);
            Assert.assertEquals(MurmurHash3.murmurhash3(s, 16777216), MurmurHash3.murmurhash3(s));
        }
    }

}

package com.juliasoft.hbase;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MiscTests {

    @Test
    public void testMacStringConversions() {
        long mac = 105681998890865L;
        String s = LongToMac.longToMac(mac);
        System.out.println(s);
        assertThat(s, is("60:1e:02:00:c7:71"));
    }

    @Test
    public void testFlip() {
        assertThat(flip((byte) -119), is((byte) 9));

        assertThat(flip((byte) -126), is((byte) 2));
        assertThat(flip((byte) -126), is((byte) 2));
        assertThat(flip((byte) -126), is((byte) 2));
        assertThat(flip((byte) -126), is((byte) 2));
        assertThat(flip((byte) 125), is((byte) -3));

        assertThat(flip((byte) 126), is((byte) -2));
        assertThat(flip((byte) 126), is((byte) -2));
        assertThat(flip((byte) 126), is((byte) -2));

        assertThat(flip((byte) 127), is((byte) -1));
        assertThat(flip((byte) 127), is((byte) -1));
        assertThat(flip((byte) 127), is((byte) -1));
        assertThat(flip((byte) 127), is((byte) -1));
        assertThat(flip((byte) 127), is((byte) -1));

        assertThat(flip((byte) -128), is((byte) 0));
        assertThat(flip((byte) -128), is((byte) 0));
        assertThat(flip((byte) -128), is((byte) 0));

        assertThat(flip((byte) -128), is((byte) 0));
        assertThat(flip((byte) -127), is((byte) 1));
    }

    private static byte flip(byte b) {
        return (byte) (b ^ (byte) 0x80);
    }
}

package com.juliasoft.hbase;

import org.junit.*;

import java.sql.*;
import java.util.Date;
import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class UDFTest {

    private static String url;
    private static Properties properties;

    @BeforeClass
    public static void setUp() throws Exception {
        url = "jdbc:phoenix:localhost:2181/hbase";

        properties = new Properties();

        properties.setProperty("user", "anything");
        properties.setProperty("password", "anything");

        properties.setProperty("hbase.client.scanner.timeout.period", "1800000");
        properties.setProperty("hbase.client.operation.timeout", "1800000");
        properties.setProperty("hbase.rpc.timeout", "1800000");
        properties.setProperty("phoenix.query.timeoutMs", "1800000");
        properties.setProperty("phoenix.query.keepAliveMs", "1800000");

        properties.setProperty("phoenix.functions.allowUserDefinedFunctions", "true");

        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");

        final Connection conn = DriverManager.getConnection(url, properties);

        execSilent(conn, "CREATE FUNCTION FLIPBYTE(TINYINT) RETURNS TINYINT" +
                "\nAS 'com.netris.phoenix.udf.FlipByte'");

        execSilent(conn, "CREATE FUNCTION DATETOINV(TIMESTAMP) RETURNS UNSIGNED_LONG" +
                "\nAS 'com.netris.phoenix.udf.DateToInv'");

        execSilent(conn, "CREATE FUNCTION INVTODATE(UNSIGNED_LONG) RETURNS TIMESTAMP" +
                "\nAS 'com.netris.phoenix.udf.InvToDate'");

        execSilent(conn, "CREATE FUNCTION LONGTOMAC(UNSIGNED_LONG) RETURNS VARCHAR" +
                "\nAS 'com.netris.phoenix.udf.LongToMac'");

        execSilent(conn, "CREATE FUNCTION MACTOLONG(VARCHAR) RETURNS UNSIGNED_LONG" +
                "\nAS 'com.netris.phoenix.udf.MacToLong'");

        execSilent(conn, "CREATE TABLE IF NOT EXISTS TEST_MAC(pk UNSIGNED_LONG NOT NULL PRIMARY KEY)");
        execSilent(conn, "CREATE TABLE IF NOT EXISTS TEST_DATE(pk UNSIGNED_LONG NOT NULL PRIMARY KEY)");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        final Connection conn = DriverManager.getConnection(url, properties);
        try {
            execSilent(conn, "DROP TABLE TEST_MAC");
            execSilent(conn, "DROP TABLE TEST_DATE");
            execSilent(conn, "DROP FUNCTION FLIPBYTE");
            execSilent(conn, "DROP FUNCTION LONGTOMAC");
            execSilent(conn, "DROP FUNCTION MACTOLONG");
            execSilent(conn, "DROP FUNCTION INVTODATE");
            execSilent(conn, "DROP FUNCTION DATETOINV");
        } finally {
            conn.commit();
        }
    }

    private static void execSilent(Connection conn, String sql) {
        try {
            conn.createStatement().execute(sql);
        } catch (Exception ex) {
            System.out.println("SILENT ERROR: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    @Test
    public void testInvertedDate() throws Exception {
        final Connection conn = DriverManager.getConnection(url, properties);

        long now = new Date().getTime();
        long inverted = Long.MAX_VALUE - now;

        final String dml = "UPSERT INTO TEST_DATE VALUES(" + inverted + ")";
        conn.createStatement().execute(dml);
        conn.commit();

        final ResultSet rs1 = conn.createStatement().executeQuery("SELECT INVTODATE(pk) FROM TEST_DATE");
        assertTrue(rs1.next());
        assertThat(rs1.getTimestamp(1), is(new Timestamp(now)));
        assertFalse(rs1.next());

        final ResultSet rs2 = conn.createStatement().executeQuery("SELECT INVTODATE(st) FROM \"ivisionevents\"");
        for (int i = 0; i < 10; i++) {
            assertTrue(rs2.next());
            final Timestamp timestamp = rs2.getTimestamp(1);
            System.out.println(timestamp);
        }
    }

    @Test
    public void testDateToInvAndBack() throws SQLException {
        final Connection conn = DriverManager.getConnection(url, properties);

        final String dml = "SELECT DATETOINV(INVTODATE(st)), st FROM \"ivisionevents\"";
        final ResultSet rs1 = conn.createStatement().executeQuery(dml);
        assertTrue(rs1.next());
        long a = rs1.getLong(1);
        long b = rs1.getLong(2);
        assertThat(a, is(b));
        assertTrue(a < Long.MAX_VALUE);
        assertTrue(a > 0);
        System.out.println(a + " " + b);
    }

    @Test
    public void testFlipByte() throws SQLException {
        final Connection conn = DriverManager.getConnection(url, properties);

        final String dml = "SELECT FLIPBYTE(pt) FROM \"ivisionevents\"";
        final ResultSet rs1 = conn.createStatement().executeQuery(dml);
        for (int i = 0; i < 10; i++) {
            assertTrue(rs1.next());
            final byte v = rs1.getByte(1);
            assertTrue(v > -20 && v < 20);
            System.out.println(v);
        }
    }

    @Test
    public void testCustomQuery() throws SQLException {
        final Connection conn = DriverManager.getConnection(url, properties);

        final String dml = "select INVTODATE(st), FLIPBYTE(pt), LONGTOMAC(mac) FROM \"ivisionevents\"";
        final ResultSet rs1 = conn.createStatement().executeQuery(dml);
        for (int i = 0; i < 10; i++) {
            assertTrue(rs1.next());
            System.out.println(rs1.getString(1));
            final byte v = rs1.getByte(2);
            assertTrue(v > -20 && v < 20);
            System.out.println(v);
            System.out.println(rs1.getString(3));
        }
    }

    @Test
    public void testLongToMacAndBack() throws Exception {
        final Connection conn = DriverManager.getConnection(url, properties);

        long mac = 185788660773111L;

        final String dml = "UPSERT INTO TEST_MAC VALUES(" + mac + ")";
        conn.createStatement().execute(dml);

        conn.commit();

        final ResultSet rs1 = conn.createStatement().executeQuery("SELECT LONGTOMAC(pk) FROM TEST_MAC");
        assertTrue(rs1.next());
        assertThat(rs1.getString(1), is("a8:f9:4b:20:f0:f7"));
        assertFalse(rs1.next());

        final ResultSet rs2 = conn.createStatement().executeQuery("SELECT MACTOLONG(LONGTOMAC(pk)) FROM TEST_MAC");
        assertTrue(rs2.next());
        assertThat(rs2.getLong(1), is(mac));
        assertFalse(rs2.next());
    }

}
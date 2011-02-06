package com.g414.haildb;

import org.testng.annotations.Test;

import com.g414.haildb.Database;

@Test
public class G414InnoDBCreateDropDBTest {
    private static String DATABASE_NAME = "foo";

    public void testCreateDrop() throws Exception {
        Database d = new Database();

        d.dropDatabase(DATABASE_NAME + "/");

        d.createDatabase(DATABASE_NAME);

        d.dropDatabase(DATABASE_NAME + "/");
    }
}

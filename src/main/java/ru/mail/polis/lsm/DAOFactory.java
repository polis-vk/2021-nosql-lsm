package ru.mail.polis.lsm;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

public class DAOFactory {

    public static DAO create(DAOConfig config) throws IOException {
        return new DaoImpl(config);
    }

}

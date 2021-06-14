package ru.mail.polis.lsm;

import java.io.IOException;

public class DAOFactory {

    private DAOFactory() {
        // Only static methods
    }

    public static DAO create(DAOConfig config) throws IOException {
        return new DaoImpl(config);
    }

}

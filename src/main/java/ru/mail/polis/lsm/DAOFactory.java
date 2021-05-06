package ru.mail.polis.lsm;

import java.io.IOException;

public class DAOFactory {

    public static DAO create(DAOConfig config) throws IOException {
        return new MyDAOImplementation(config);
    }

}

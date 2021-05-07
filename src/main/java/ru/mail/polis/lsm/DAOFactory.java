package ru.mail.polis.lsm;

import java.io.IOException;

public class DAOFactory {

    private DAOFactory() {
    }

    public static DAO create(DAOConfig config) throws IOException {
        throw new UnsupportedOperationException("Implement me");
    }

}

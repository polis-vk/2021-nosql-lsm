package ru.mail.polis.lsm;

import ru.mail.polis.lsm.ponomarev.PonomarevDAO;

import java.io.IOException;

public final class DAOFactory {

    private DAOFactory() {
        // Only static methods
    }

    /**
     * Create an instance of {@link DAO} with supplied {@link DAOConfig}.
     */
    public static DAO create(DAOConfig config) throws IOException {
        return new PonomarevDAO(config);
    }
}

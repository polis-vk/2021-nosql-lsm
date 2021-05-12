package ru.mail.polis.lsm;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

public final class DAOFactory {

    private DAOFactory() {
        // Only static methods
    }

    /**
     * Create an instance of {@link DAO} with supplied {@link DAOConfig}.
     */
    public static DAO create(DAOConfig config) throws IOException {
        assert config.getDir().toFile().exists();

        return new DaoImpl(config);
    }

}

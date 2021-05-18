package ru.mail.polis.lsm;

import ru.mail.polis.lsm.ilia.DAOImpl;

import java.io.IOException;

public final class DAOFactory {

    private DAOFactory() {
        // Only static methods
    }

    /**
     * Create an instance of {@link DAO} with supplied {@link DAOConfig} (unused yet).
     */
    public static DAO create(DAOConfig config) throws IOException {
        assert config.getDir().toFile().exists();

        return new DAOImpl(config);
    }
}

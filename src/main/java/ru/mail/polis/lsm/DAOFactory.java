package ru.mail.polis.lsm;

import ru.mail.polis.lsm.ponomarev_stepan.SimpleMemoryFileDAO;

import java.io.IOException;

public final class DAOFactory {

    private DAOFactory() {
        // Only static methods
    }

    /**
     * Create an instance of {@link DAO} with supplied {@link DAOConfig} (unused yet).
     */
    public static DAO create(DAOConfig config) throws IOException {
        return new SimpleMemoryFileDAO(config);
    }
}

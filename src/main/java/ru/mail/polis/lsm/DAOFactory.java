package ru.mail.polis.lsm;

import ru.mail.polis.lsm.saveliy_schur.InMemoryDAO;

import java.io.IOException;

public class DAOFactory {

    private DAOFactory() {
        // Only static methods
    }

    public static DAO create(DAOConfig config) throws IOException {
        assert config.getDir().toFile().exists();

        return new InMemoryDAO(config);
    }

}

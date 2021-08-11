package ru.mail.polis.lsm;

import ru.mail.polis.lsm.saveliyschur.sstservice.LSMDao;

import java.io.IOException;

public final class DAOFactory {

    private DAOFactory() {
        // Only static methods
    }

    public static DAO create(DAOConfig config) throws IOException {
        assert config.getDir().toFile().exists();

        return new LSMDao(config);
    }

}

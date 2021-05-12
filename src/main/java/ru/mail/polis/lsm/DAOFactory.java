package ru.mail.polis.lsm;

import ru.mail.polis.lsm.vadimershov.InMemoryDAO;

public final class DAOFactory {

    private DAOFactory() {
        // Only static methods
    }

    /**
     * Create an instance of {@link DAO} with supplied {@link DAOConfig}.
     */
    public static DAO create(DAOConfig config) {
        assert config.getDir().toFile().exists();

        return new InMemoryDAO();
    }

}

package ru.mail.polis.lsm;

import ru.mail.polis.lsm.vadimershov.InMemoryDAO;

public class DAOFactory {

    public static DAO create(DAOConfig config) {
        return new InMemoryDAO();
    }

}

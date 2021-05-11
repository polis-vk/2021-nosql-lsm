package ru.mail.polis.lsm;

import ru.mail.polis.lsm.vadim_ershov.InMemoryDAO;

import java.io.IOException;

public class DAOFactory {

    public static DAO create(DAOConfig config) {
        return new InMemoryDAO();
    }

}

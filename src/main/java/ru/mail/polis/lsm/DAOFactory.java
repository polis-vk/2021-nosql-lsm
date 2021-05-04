package ru.mail.polis.lsm;

import ru.mail.polis.lsm.gromov_maxim.InMemoryDAO;

import java.io.IOException;

public class DAOFactory {

    public static DAO create(DAOConfig config) throws IOException {
        return new InMemoryDAO();
    }

}

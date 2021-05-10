package ru.mail.polis.lsm;

import ru.mail.polis.lsm.roman_marasanov.InMemoryDAO;

import java.io.IOException;

public class DAOFactory {

    public static DAO create(DAOConfig config) throws IOException {
        return new InMemoryDAO();
    }

}

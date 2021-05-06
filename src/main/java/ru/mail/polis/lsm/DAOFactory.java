package ru.mail.polis.lsm;

import ru.mail.polis.lsm.segu.InMemoryImpl;

import java.io.IOException;

public class DAOFactory {

    public static DAO create(DAOConfig config) throws IOException {
        return new InMemoryImpl();
    }

}

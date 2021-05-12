package ru.mail.polis.lsm;

import java.io.IOException;
import ru.mail.polis.lsm.igor_samokhin.InMemoryDAO;

public class DAOFactory {

    public static DAO create(DAOConfig config) throws IOException {
        return new InMemoryDAO(config);
    }

}

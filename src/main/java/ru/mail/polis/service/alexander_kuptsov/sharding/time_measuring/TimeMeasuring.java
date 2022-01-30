package ru.mail.polis.service.alexander_kuptsov.sharding.time_measuring;

import ru.mail.polis.service.alexander_kuptsov.sharding.time_measuring.measures.AddServerTimeMeasure;
import ru.mail.polis.service.alexander_kuptsov.sharding.time_measuring.measures.RemoveServerTimeMeasure;
import ru.mail.polis.service.alexander_kuptsov.sharding.time_measuring.measures.ServerByIdTimeMeasure;
import ru.mail.polis.service.alexander_kuptsov.sharding.time_measuring.measures.TimeMeasure;

public final class TimeMeasuring {

    private TimeMeasuring() {
    }

    public static void main(String[] args) {
        TimeMeasure serverByIdTimeMeasure = new ServerByIdTimeMeasure();
        serverByIdTimeMeasure.collectResults();

        TimeMeasure addServerTimeMeasure = new AddServerTimeMeasure();
        addServerTimeMeasure.collectResults();

        TimeMeasure removeServerTimeMeasure = new RemoveServerTimeMeasure();
        removeServerTimeMeasure.collectResults();
    }
}

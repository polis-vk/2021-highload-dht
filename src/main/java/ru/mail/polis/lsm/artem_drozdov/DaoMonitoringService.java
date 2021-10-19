package ru.mail.polis.lsm.artem_drozdov;

public interface DaoMonitoringService {

    default void onFlushStarted() {
    }

    default void onFlushFinished() {
    }

    default void onUpsertStarted() {
    }

    default void onUpsertFinished() {
    }

    default void onStateChanged(DAOState newState) {

    }
}

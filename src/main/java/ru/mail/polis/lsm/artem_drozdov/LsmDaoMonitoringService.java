package ru.mail.polis.lsm.artem_drozdov;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class LsmDaoMonitoringService implements DaoMonitoringService {

    private final Logger LOG = LoggerFactory.getLogger(LsmDaoMonitoringService.class);

    private final LsmDAO dao;

    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();

    private static final int FLUSHES_TASKS_TO_RESUME = LsmDAO.FLUSH_TASKS_LIMIT / 2;



    public LsmDaoMonitoringService(LsmDAO dao) {
        this.dao = dao;
    }

    @Override
    public void onStateChanged(DAOState newState) {
        if (newState == DAOState.FLUSH_TASKS_LIMIT) {

            waitForFlushTasksReduction();

            throw new IllegalStateException("Unexpected DAO state");
        }
    }

    private void waitForFlushTasksReduction() {
        ScheduledFuture<?> scheduledFuture = scheduledExecutor.scheduleWithFixedDelay(() -> {
            if (dao.getState() == DAOState.FLUSH_TASKS_LIMIT) {
                dao.setState(DAOState.OK);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);

    }
}

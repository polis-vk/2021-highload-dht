package ru.mail.polis.service.eldar_tim.handlers;

import ru.mail.polis.Cluster;
import ru.mail.polis.service.eldar_tim.ServiceExecutor;
import ru.mail.polis.sharding.HashRouter;

public class HandlerContext {

    public final Cluster.Node self;
    public final HashRouter<Cluster.Node> router;
    public final Cluster.ReplicasHolder replicasHolder;
    public final ServiceExecutor workers;
    public final ServiceExecutor proxies;

    public HandlerContext(
            Cluster.Node self, HashRouter<Cluster.Node> router,
            Cluster.ReplicasHolder replicasHolder,
            ServiceExecutor workers, ServiceExecutor proxies) {

        this.self = self;
        this.router = router;
        this.replicasHolder = replicasHolder;
        this.workers = workers;
        this.proxies = proxies;
    }
}

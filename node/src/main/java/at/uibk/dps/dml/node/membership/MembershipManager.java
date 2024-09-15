package at.uibk.dps.dml.node.membership;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.spi.cluster.NodeInfo;
import io.vertx.ext.cluster.infinispan.InfinispanClusterManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MembershipManager {

    private final Logger logger = LoggerFactory.getLogger(MembershipManager.class);

    private final Vertx vertx;

    private final InfinispanClusterManager clusterManager;

    private final SplitBrainHandler splitBrainHandler;

    private final List<MembershipChangeListener> changeListeners = new ArrayList<>();

    /**
     * Maps node IDs to node infos.
     */
    private final Map<String, DmlNodeInfo> nodeInfoMap = new HashMap<>();

    private final ClusterView ispnClusterView = new ClusterView();

    private MembershipView membershipView = null;

    private Promise<Void> initPromise = null;

    /**
     * Creates a new membership manager.
     *
     * @param vertx the vertx instance
     * @param clusterManager the cluster manager
     * @param splitBrainHandler the split brain handler
     */
    public MembershipManager(Vertx vertx, InfinispanClusterManager clusterManager,
                             SplitBrainHandler splitBrainHandler) {
        this.vertx = vertx;
        this.clusterManager = clusterManager;
        this.splitBrainHandler = splitBrainHandler;
    }

    /**
     * Initializes the membership manager.
     *
     * @return a future that completes when the membership manager is initialized
     */
    @SuppressWarnings("deprecation")
    public Future<Void> init() {
        initPromise = Promise.promise();
        Future<Void> initFuture = initPromise.future();
        EmbeddedCacheManager ispnCacheManager = (EmbeddedCacheManager) clusterManager.getCacheContainer();
        ispnClusterView.epoch = ispnCacheManager.getTransport().getViewId();
        ispnClusterView.members = ispnCacheManager.getMembers().stream().distinct().map(Object::toString).collect(Collectors.toList());
        ispnCacheManager.addListener(new ClusterViewListener());
        ispnClusterView.members.forEach(nodeId -> getNodeInfo(nodeId, System.currentTimeMillis()));
        return initFuture;
    }

    /**
     * Adds a listener that is notified when the membership view changes.
     *
     * @param changeListener the listener to add
     */
    public void addListener(MembershipChangeListener changeListener) {
        synchronized (changeListeners) {
            changeListeners.add(changeListener);
        }
    }

    /**
     * Returns the current membership view.
     *
     * @return the current membership view
     */
    public MembershipView getMembershipView() {
        return membershipView;
    }

    @Listener(sync = false)
    private class ClusterViewListener {
        @Merged
        public void handleViewMerge(ViewChangedEvent viewChangedEvent) {
            // Note that this is called by an Infinispan thread
            synchronized (this) {
                splitBrainHandler.handle();
            }
        }

        @ViewChanged
        public void handleViewChange(ViewChangedEvent viewChangedEvent) {
            // Note that this is called by an Infinispan thread
            synchronized (this) {
                if (viewChangedEvent.getViewId() <= ispnClusterView.epoch) {
                    return;
                }

                List<Address> removedMembers = new ArrayList<>(viewChangedEvent.getOldMembers());
                removedMembers.removeAll(viewChangedEvent.getNewMembers());
                if (removedMembers.size() >= Math.ceil(viewChangedEvent.getOldMembers().size() / 2.0)) {
                    splitBrainHandler.handle();
                }

                ispnClusterView.epoch = viewChangedEvent.getViewId();
                ispnClusterView.members = viewChangedEvent.getNewMembers().stream().distinct().map(Object::toString).collect(Collectors.toList());

                List<Address> addedMembers = new ArrayList<>(viewChangedEvent.getNewMembers());
                addedMembers.removeAll(viewChangedEvent.getOldMembers());
                for (Address addedMember : addedMembers) {
                    getNodeInfo(addedMember.toString(), System.currentTimeMillis());
                }

                for (Address removedMember : removedMembers) {
                    nodeInfoMap.remove(removedMember.toString());
                }
            }
            checkViewUpdate();
        }
    }

    private void getNodeInfo(String nodeId, long startTime) {
        Promise<NodeInfo> promise = Promise.promise();
        clusterManager.getNodeInfo(nodeId, promise);
        promise.future().onComplete(asyncResult -> {
            if (!ispnClusterView.members.contains(nodeId)) {
                logger.warn("Node {} left while obtaining its metadata", nodeId);
                checkViewUpdate();
                return;
            }
            if (asyncResult.succeeded()) {
                NodeInfo nodeInfo = asyncResult.result();
                if (nodeInfo.metadata() != null && !nodeInfo.metadata().isEmpty()) {
                    DmlNodeInfo dmlNodeInfo = nodeInfo.metadata().mapTo(DmlNodeInfo.class);
                    synchronized (this) {
                        nodeInfoMap.put(nodeId, dmlNodeInfo);
                    }
                    checkViewUpdate();
                    return;
                }
            }
            // It takes some time until the node info is available after a node joins
            vertx.setTimer(200, timerId -> {
                long newStartTime = startTime;
                if (System.currentTimeMillis() - startTime > 120000) {
                    logger.error("Could not obtain metadata of joined node " + nodeId, asyncResult.cause());
                    newStartTime = System.currentTimeMillis();
                }
                getNodeInfo(nodeId, newStartTime);
            });
        });
    }

    private void checkViewUpdate() {
        MembershipView newMembershipView;
        synchronized (this) {
            if (!nodeInfoMap.keySet().containsAll(ispnClusterView.members)) {
                return;
            }
            Map<String, DmlNodeInfo> nodeInfoOfMembers = ispnClusterView.members.stream().distinct().collect(Collectors.toMap(nodeId -> nodeId, nodeInfoMap::get, (info1, info2) -> info1));
            newMembershipView = new MembershipView(ispnClusterView.epoch, nodeInfoOfMembers);
            membershipView = newMembershipView;

            if (initPromise != null) {
                if (Vertx.currentContext() != vertx.getOrCreateContext()) {
                    final Promise<Void> finalInitPromise = this.initPromise;
                    vertx.runOnContext(v -> finalInitPromise.complete());
                } else {
                    initPromise.complete();
                }
                this.initPromise = null;
            }
        }

        logger.info("New membership view: {}", newMembershipView);

        synchronized (changeListeners) {
            for (MembershipChangeListener listener : changeListeners) {
                listener.membershipChanged(newMembershipView);
            }
        }
    }

    static class ClusterView {
        int epoch;
        List<String> members;
    }
}

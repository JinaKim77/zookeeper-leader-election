import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    public static final String ELECTION_NAMESPACE = "/election";
    private String currentZnodeName;

    private ZooKeeper zooKeeper;
    private boolean leader = false;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();
        leaderElection.connectToZookeeper();
        leaderElection.volunteerForLeadership();
        leaderElection.reElectLeader();
        leaderElection.run();
        System.out.println("Disconnected from Zookeeper, exiting application");

    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);

    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
        System.out.println("znode name " + currentZnodeName);

    }

    public void reElectLeader() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
        Collections.sort(children);
        String smallestChild = children.get(0);

        if (smallestChild.equals(currentZnodeName)) {
            System.out.println("I am the leader");
            leader = true;
            return;
        }
        System.out.println("I am not the leader, " + smallestChild + " is the leader");
        leader = false;
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
        }
    }

    //******************************
    // Needed for tests, don't edit
    //*******************************

    public boolean isLeader() {
        return leader;
    }
}

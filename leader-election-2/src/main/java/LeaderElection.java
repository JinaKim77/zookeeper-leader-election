import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

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
        String predecessorName ="";
        Stat predecessorStat = null;

        while (predecessorStat==null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.equals(currentZnodeName)) {
                System.out.println("I am the leader");
                leader = true;
                return;
            }else {
                System.out.println("I am not the leader, " + smallestChild + " is the leader");
                leader = false;

                //find out what the index of the node just before the current node
                int precedessorIndex = Collections.binarySearch(children, currentZnodeName) -1;
                predecessorName = children.get(precedessorIndex);

                //Each node is going to watch the node in front of it in the list
                //This class will watch any changes on this znode
                //exists returns an object type Stat!
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorName,  this);
            }
        }
        System.out.println("Watching znode : "+predecessorName);
        System.out.println();
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
                break;

            case NodeDeleted:
                try {
                    System.out.println("Received node deletion event");
                    reElectLeader();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
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

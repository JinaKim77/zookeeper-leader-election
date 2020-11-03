import com.github.blindpirate.extensions.CaptureSystemOutput;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.*;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.junit.jupiter.api.Assertions.*;

class LeaderElectionTest {
    private static TestingServer zkServer;
    private static ZooKeeper zooKeeper;
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    public static final String ELECTION_NAMESPACE = "/election";

    @Test
    @CaptureSystemOutput
    public void processConnected(CaptureSystemOutput.OutputCapture outputCapture) throws IOException, InterruptedException {
        LeaderElection node = new LeaderElection();
        outputCapture.expect(containsStringIgnoringCase("connected"));
        node.connectToZookeeper();
        node.close();
    }

    @Test
    @CaptureSystemOutput
    public void processDisConnected(CaptureSystemOutput.OutputCapture outputCapture) throws IOException, InterruptedException {
        LeaderElection node = new LeaderElection();
        node.connectToZookeeper();
        outputCapture.expect(containsStringIgnoringCase("disconnected"));
        node.close();
    }

    @Test
    void volunteerForLeadership() throws KeeperException, InterruptedException, IOException {
        List<LeaderElection> nodes = createNodes(1);
        setupNodes(nodes);

        List<String> electionChildren = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

        Assertions.assertAll(
                () -> assertEquals(1, electionChildren.size(), "One znode should be created under the /election parent"),
                () -> assertEquals("c_0000000000", electionChildren.get(0), "The created znode has the wrong name")
        );
        stopNodes(nodes);
    }

    @Test
    void electLeaderOneNode() throws IOException, KeeperException, InterruptedException {
        List<LeaderElection> nodes = createNodes(1);
        setupNodes(nodes);

        List<String> electionChildren = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

        assertTrue(nodes.get(0).isLeader(), "The leader has not been elected correctly");

        stopNodes(nodes);
    }

    @Test
    void electLeaderTwoNodes() throws IOException, KeeperException, InterruptedException {
        List<LeaderElection> nodes = createNodes(2);
        setupNodes(nodes);

        List<String> electionChildren = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

        Assertions.assertAll(
                () -> assertEquals(2, electionChildren.size(), "Two znodes should have been created under the /election parent"),
                () -> assertTrue(nodes.get(0).isLeader(), "The leader has not been elected correctly, the expected node is not the leader"),
                () -> assertFalse(nodes.get(1).isLeader(), "The leader has not been elected correctly, the expected node is not the leader")
        );
        stopNodes(nodes);
    }

    @Test
    void basicReelection() throws IOException, KeeperException, InterruptedException {
        List<LeaderElection> nodes = createNodes(2);
        setupNodes(nodes);

        Assertions.assertAll(
                () -> assertTrue(nodes.get(0).isLeader(), "The leader has not been elected correctly, the expected node is not the leader"),
                () -> assertFalse(nodes.get(1).isLeader(), "The leader has not been elected correctly, the expected node is not the leader")
        );

        // Kill node 1, node 2 should take over
        nodes.get(0).close();
        Thread.sleep(500);

        assertTrue(nodes.get(1).isLeader(), "The expected node has not taken over leadership");
        stopNodes(nodes);
    }

    @Test
    void advancedReelection() throws IOException, KeeperException, InterruptedException {
        List<LeaderElection> nodes = createNodes(4);
        setupNodes(nodes);

        Assertions.assertAll(
                () -> assertTrue(nodes.get(0).isLeader(), "The leader has not been elected correctly, the expected node is not the leader"),
                () -> assertFalse(nodes.get(1).isLeader(), "The leader has not been elected correctly, the expected node is not the leader"),
                () -> assertFalse(nodes.get(2).isLeader(), "The leader has not been elected correctly, the expected node is not the leader"),
                () -> assertFalse(nodes.get(3).isLeader(), "The leader has not been elected correctly, the expected node is not the leader")
        );

        // Kill node 2, node 3 should start watching node 1
        nodes.get(1).close();
        Thread.sleep(500);
        // Kill node 1, node 3 should take over leadership
        nodes.get(0).close();
        Thread.sleep(500);

        assertTrue(nodes.get(2).isLeader(), "The expected node has not taken over leadership");
        stopNodes(nodes);
    }

    @BeforeAll
    public static void setUp() throws Exception {
        zkServer = new TestingServer(2181, true);
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, watchedEvent -> {
        });
    }

    @BeforeEach
    private void createElectionZnode() throws KeeperException, InterruptedException {
        zooKeeper.create(ELECTION_NAMESPACE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @AfterEach
    private void deleteZnodes() throws KeeperException, InterruptedException {
        ZKUtil.deleteRecursive(zooKeeper, ELECTION_NAMESPACE);
    }

    @AfterAll
    public static void tearDown() throws Exception {
        zkServer.stop();
    }


    private List<LeaderElection> createNodes(int numberOfNodes) {
        List<LeaderElection> nodes = new ArrayList<>();
        for (int i = 0; i < numberOfNodes; i++) {
            nodes.add(new LeaderElection());
        }
        return nodes;
    }

    private void setupNodes(List<LeaderElection> nodes) throws IOException, KeeperException, InterruptedException {
        for (LeaderElection node : nodes) {
            node.connectToZookeeper();
            node.volunteerForLeadership();
            node.reElectLeader();
            startNode(node);
        }
    }

    private void startNode(LeaderElection node) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    node.run();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    private void stopNodes(List<LeaderElection> nodes) throws InterruptedException {
        for (LeaderElection node : nodes) {
            node.close();
        }
    }
}
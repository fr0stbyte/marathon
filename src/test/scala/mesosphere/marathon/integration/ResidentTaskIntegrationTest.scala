package mesosphere.marathon.integration

import mesosphere.marathon.api.v2.json.AppUpdate
import mesosphere.marathon.integration.setup.MarathonFacade._
import mesosphere.marathon.integration.setup.{ ITEnrichedTask, IntegrationFunSuite, SingleMarathonIntegrationTest }
import mesosphere.marathon.state._
import org.apache.mesos.{ Protos => Mesos }
import org.scalatest.{ BeforeAndAfter, GivenWhenThen, Matchers }

import scala.collection.immutable.Seq
import scala.util.Try

class ResidentTaskIntegrationTest
    extends IntegrationFunSuite
    with SingleMarathonIntegrationTest
    with Matchers
    with BeforeAndAfter
    with GivenWhenThen {

  //clean up state before running the test case
  before(cleanUp())

  test("resident task can be deployed and write to persistent volume") {
    Given("An app that writes into a persistent volume")
    val containerPath = "persistent-volume"
    val app = F.residentApp(
      containerPath = containerPath,
      cmd = s"""echo "data" > $containerPath/data ; exit 0 ;""")

    When("A task is launched")
    F.create(app)

    Then("It writes successfully to the persistent volume and finishes")
    waitForStatusUpdates(F.StatusUpdate.TASK_RUNNING, F.StatusUpdate.TASK_FINISHED)
  }

  test("persistent volume will be re-attached and keep state") {
    Given("An app that writes into a persistent volume")
    val containerPath = "persistent-volume"
    val app = F.residentApp(
      containerPath = containerPath,
      cmd = s"""echo "data" > $containerPath/data """)

    When("a task is launched")
    F.create(app)

    Then("it successfully writes to the persistent volume and then finishes")
    waitForStatusUpdates(F.StatusUpdate.TASK_RUNNING, F.StatusUpdate.TASK_FINISHED)

    When("the app is suspended")
    F.suspend(app.id)

    And("a new task is started that checks for the previously written file")
    // deploy a new version that checks for the data written the above step
    F.update(app.id, AppUpdate(instances = Some(1),
      cmd = Some(s"""if [ -e $containerPath/data ] ; then exit 0 ; else exit 1 ; fi ;""")))

    Then("the new task verifies that the persistent volume file is still there")
    waitForStatusUpdates(F.StatusUpdate.TASK_RUNNING, F.StatusUpdate.TASK_FINISHED)
  }

  test("Scale Up") {
    Given("A resident app with 0 instances")
    val app = F.create(F.residentApp(instances = 0))

    When("We scale up to 5 instances")
    F.scaleTo(app.id, 5)

    Then("exactly 5 tasks have been created")
    F.launchedTasks(app.id).size shouldBe 5
  }

  test("Scale Down") {
    Given("a resident app with 5 instances")
    val app = F.create(F.residentApp(instances = 5))

    When("we scale down to 0 instances")
    F.suspend(app.id)

    Then("all tasks are suspended")
    val all = F.allTasks(app.id)
    all.size shouldBe 5
    all.count(_.launched) shouldBe 0
    all.count(_.suspended) shouldBe 5
  }

  test("Restart") {
    Given("a resident app with 5 instances and upgradeStrategy(0.5, 0)")
    val app = F.create(F.residentApp(instances = 5, upgradeStrategy = UpgradeStrategy(0.5, 0.0)))

    When("we restart the app")
    val newVersion = F.restart(app)
    val all = F.allTasks(app.id)

    And("all 5 tasks are restarted and of the new version")
    all.map(_.version).forall(_.contains(newVersion)) shouldBe true

    And("exactly 5 instances are running")
    all.count(_.launched) shouldBe 5

    And("no extra task was created")
    all.size shouldBe 5
  }

  test("Config Change") {
    Given("a resident app with 5 instances and upgradeStrategy(0.5, 0)")
    val app = F.create(F.residentApp(instances = 5, upgradeStrategy = UpgradeStrategy(0.5, 0.0)))

    When("we change the config")
    val newVersion = F.update(app.id, AppUpdate(cmd = Some("sleep 1234"))).toString
    val all = F.allTasks(app.id)

    Then("all 5 tasks are of the new version")
    all.map(_.version).forall(_.contains(newVersion)) shouldBe true

    And("exactly 5 instances are running")
    all.count(_.launched) shouldBe 5

    And("no extra task was created")
    all.size shouldBe 5
  }

  /**
    * FIXME (3043): implement the following tests. TASK_LOST can be induced when launching a task with permission:
    *
    * When a framework launches a task, “run_tasks” ACLs are checked to see if the framework
    * (FrameworkInfo.principal) is authorized to run the task/executor as the given user. If not authorized,
    * the launch is rejected and the framework gets a TASK_LOST.
    *
    * (From http://mesos.apache.org/documentation/latest/authorization/)
    */

  ignore("taskLostBehavior = RELAUNCH_AFTER_TIMEOUT, timeout = 10s") {
    Given("A resident app with 1 instance")
    When("The task is lost")
    Then("The task is not relaunched within the timeout")
    And("The task is relaunched with a new Id after the timeout")
  }

  ignore("taskLostBehavior = WAIT_FOREVER") {
    Given("A resident app with 1 instance")
    When("The task is lost")
    Then("No timeout is scheduled") // can we easily verify this?
    And("The task is not relaunched") // can we verify this without waiting?
  }

  ignore("relaunchEscalationTimeoutSeconds = 5s") {
    Given("A resident app with 1 instance")
    When("The task terminates")
    And("We don't get an offer within the timeout")
    Then("We launch a new task on any matching offer")
  }

  ignore("Persistent volumes are not destroyed when an app is destroyed") {
    Given("a resident app with 1 running instance")
    When("the app is deleted")
    Then("the task gets killed")
    And("the task is still stored")
    When("the app is created again")
    Then("the existing task will be picked up again")
    And("the new task will use the existing persistent volume")
  }

  object F {
    type VersionString = String

    object Event {
      val STATUS_UPDATE_EVENT = "status_update_event"
      val DEPLOYMENT_SUCCESS = "deployment_success"
    }

    object StatusUpdate {
      val TASK_FINISHED = "TASK_FINISHED"
      val TASK_RUNNING = "TASK_RUNNING"
      val TASK_FAILED = "TASK_FAILED"
    }

    /**
      * Resident Tasks reside in the TaskTracker even after they terminate and after the associated app is deleted.
      * To prevent spurious state in the above test cases, each test case should use a unique appId.
      */
    private[this] object IdGenerator {
      private[this] var index: Int = 0
      def generate(): String = {
        index += 1
        index.toString
      }
    }

    def residentApp(
      containerPath: String = "persistent-volume",
      cmd: String = "sleep 1000",
      instances: Int = 1,
      upgradeStrategy: UpgradeStrategy = UpgradeStrategy.empty): AppDefinition = {

      val appId: PathId = PathId(s"/$testBasePath/app-${IdGenerator.generate()}")

      val persistentVolume: Volume = PersistentVolume(
        containerPath = containerPath,
        persistent = PersistentVolumeInfo(size = 1),
        mode = Mesos.Volume.Mode.RW
      )

      val app = AppDefinition(
        appId,
        instances = instances,
        residency = Some(Residency(
          Residency.defaultRelaunchEscalationTimeoutSeconds,
          Residency.defaultTaskLostBehaviour
        )),
        container = Some(Container(
          `type` = Mesos.ContainerInfo.Type.MESOS,
          volumes = Seq(persistentVolume)
        )),
        cmd = Some(cmd),
        executor = "",
        // cpus, mem and disk are really small because otherwise we'll soon run out of reservable resources
        cpus = 0.001,
        mem = 1.0,
        disk = 1.0,
        upgradeStrategy = upgradeStrategy
      )

      app
    }

    // FIXME (3043): The following could be reused in other tests

    def create(app: AppDefinition): AppDefinition = {
      val result = marathon.createAppV2(app)
      result.code should be (201) //Created
      extractDeploymentIds(result) should have size 1
      waitForEvent(F.Event.DEPLOYMENT_SUCCESS)
      waitForTasks(app.id, app.instances)
      app
    }

    def scaleTo(appId: PathId, instances: Int): Iterable[ITEnrichedTask] = {
      val result = marathon.updateApp(appId, AppUpdate(instances = Some(instances)))
      result.code should be (200) // OK
      waitForEvent(F.Event.DEPLOYMENT_SUCCESS)
      waitForTasks(appId, instances)
    }

    def suspend(appId: PathId): Iterable[ITEnrichedTask] = scaleTo(appId, 0)

    def update(appId: PathId, update: AppUpdate): F.VersionString = {
      val result = marathon.updateApp(appId, update)
      result.code shouldBe 200
      waitForEvent(F.Event.DEPLOYMENT_SUCCESS)
      result.value.version.toString
    }

    def restart(app: AppDefinition): F.VersionString = {
      val result = marathon.restartApp(app.id)
      result.code shouldBe 200
      waitForEvent(F.Event.DEPLOYMENT_SUCCESS)
      result.value.version.toString
    }

    def allTasks(appId: PathId): Iterable[ITEnrichedTask] = {
      Try(marathon.tasks(appId)).map(_.value).getOrElse(Nil)
    }

    def launchedTasks(appId: PathId): Iterable[ITEnrichedTask] = allTasks(appId).filter(_.launched)

    def suspendedTasks(appId: PathId): Iterable[ITEnrichedTask] = allTasks(appId).filter(_.suspended)
  }

}

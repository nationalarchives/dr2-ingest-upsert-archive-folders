package testUtils

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.tomakehurst.wiremock.WireMockServer
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scanamo.DynamoFormat
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.Lambda.{GetItemsResponse, PrimaryKey}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, Identifier}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.{AddEntityRequest, UpdateEntityRequest}
import uk.gov.nationalarchives.{DADynamoDBClient, Lambda}

import java.util.UUID
import scala.collection.immutable.ListMap

class ExternalServicesTestUtils extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll {
  val graphQlServerPort = 9001

  val wiremockGraphqlServer = new WireMockServer(graphQlServerPort)

  val folderIdsAndRows: ListMap[String, GetItemsResponse] = ListMap(
    "f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176" ->
      GetItemsResponse(
        "f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176",
        "",
        "mock title_1",
        Some("mock title_1"),
        Some("mock description_1")
      ),
    "e88e433a-1f3e-48c5-b15f-234c0e663c27" -> GetItemsResponse(
      "e88e433a-1f3e-48c5-b15f-234c0e663c27",
      "f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176",
      "mock title_1_1",
      Some("mock title_1_1"),
      Some("mock description_1_1")
    ),
    "93f5a200-9ee7-423d-827c-aad823182ad2" -> GetItemsResponse(
      "93f5a200-9ee7-423d-827c-aad823182ad2",
      "f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176/e88e433a-1f3e-48c5-b15f-234c0e663c27",
      "mock title_1_1_1",
      Some("mock title_1_1_1"),
      Some("mock description_1_1_1")
    )
  )

  val structuralObjects: Map[Int, Seq[Entity]] = Map(
    0 -> Seq(
      Entity(
        Some("SO"),
        UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"),
        Some("mock title_1"),
        Some("mock description_1"),
        deleted = false,
        Some("structural-objects")
      )
    ),
    1 -> Seq(
      Entity(
        Some("SO"),
        UUID.fromString("a2d39ea3-6216-4f93-b078-62c7896b174c"),
        Some("mock title_1_1"),
        Some("mock description_1_1"),
        deleted = false,
        Some("structural-objects")
      )
    ),
    2 -> Seq(
      Entity(
        Some("SO"),
        UUID.fromString("9dfc40be-5f44-4fa1-9c25-fbe03dd3f539"),
        Some("mock title_1_1_1"),
        Some("mock description_1_1_1"),
        deleted = false,
        Some("structural-objects")
      )
    )
  )

  val defaultEntitiesWithSourceIdReturnValues: List[IO[Seq[Entity]]] =
    List(IO(structuralObjects(0)), IO(structuralObjects(1)), IO(structuralObjects(2)))

  override def beforeAll(): Unit = {
    wiremockGraphqlServer.start()
  }

  override def afterAll(): Unit = {
    wiremockGraphqlServer.stop()
  }

  override def afterEach(): Unit = {
    wiremockGraphqlServer.resetAll()
  }

  case class MockLambda(
      getAttributeValuesReturnValue: IO[List[GetItemsResponse]],
      entitiesWithSourceIdReturnValue: List[IO[Seq[Entity]]] = defaultEntitiesWithSourceIdReturnValues,
      addEntityReturnValue: IO[UUID] = IO(UUID.fromString("9dfc40be-5f44-4fa1-9c25-fbe03dd3f539")),
      addIdentifierReturnValue: IO[String] = IO("The Identifier was added"),
      getParentFolderRefAndSecurityTagReturnValue: List[IO[Map[String, String]]] = List(
        IO(Map("ParentRef" -> "562530e3-3b6e-435a-8b56-1d3ad4868a9a", "SecurityTag" -> "open")),
        IO(Map("ParentRef" -> "d7879799-a7de-4aa6-8c7b-afced66a6c50", "SecurityTag" -> "open")),
        IO(Map("ParentRef" -> "a2d39ea3-6216-4f93-b078-62c7896b174c", "SecurityTag" -> "open"))
      ),
      updateEntityReturnValues: IO[String] = IO("Entity was updated")
  ) extends Lambda() {
    val apiUrlCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    def getIdentifierToGetCaptor: ArgumentCaptor[Identifier] = ArgumentCaptor.forClass(classOf[Identifier])
    def getEntitiesSecretNameCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    def getAddFolderRequestCaptor: ArgumentCaptor[AddEntityRequest] = ArgumentCaptor.forClass(classOf[AddEntityRequest])
    def getRefCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    def structuralObjectCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    def identifiersToAddCaptor: ArgumentCaptor[List[Identifier]] = ArgumentCaptor.forClass(classOf[List[Identifier]])
    def childNodesCaptor: ArgumentCaptor[List[String]] = ArgumentCaptor.forClass(classOf[List[String]])
    def getUpdateFolderRequestCaptor: ArgumentCaptor[UpdateEntityRequest] =
      ArgumentCaptor.forClass(classOf[UpdateEntityRequest])

    val entityCaptor: ArgumentCaptor[Entity] = ArgumentCaptor.forClass(classOf[Entity])

    def getPrimaryKeysCaptor: ArgumentCaptor[List[PrimaryKey]] = ArgumentCaptor.forClass(classOf[List[PrimaryKey]])
    def getTableNameCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    val mockEntityClient: EntityClient[IO, Fs2Streams[IO]] = mock[EntityClient[IO, Fs2Streams[IO]]]
    val mockDynamoDBClient: DADynamoDBClient[IO] = mock[DADynamoDBClient[IO]]

    override val dADynamoDBClient: DADynamoDBClient[IO] = {
      when(
        mockDynamoDBClient.getItems[GetItemsResponse, PrimaryKey](any[List[PrimaryKey]], any[String])(
          any[DynamoFormat[GetItemsResponse]],
          any[DynamoFormat[PrimaryKey]]
        )
      ).thenReturn(
        getAttributeValuesReturnValue
      )

      mockDynamoDBClient
    }

    override lazy val entitiesClientIO: IO[EntityClient[IO, Fs2Streams[IO]]] = {
      when(mockEntityClient.entitiesByIdentifier(any[Identifier], any[String]))
        .thenReturn(
          entitiesWithSourceIdReturnValue.head,
          entitiesWithSourceIdReturnValue(1),
          entitiesWithSourceIdReturnValue(2)
        )
      when(mockEntityClient.addEntity(any[AddEntityRequest], any[String]))
        .thenReturn(addEntityReturnValue)
      when(mockEntityClient.addIdentifiersForEntity(any[UUID], any[String], any[List[Identifier]], any[String]))
        .thenReturn(addIdentifierReturnValue)
      when(mockEntityClient.nodesFromEntity(any[UUID], any[String], any[List[String]], any[String]))
        .thenReturn(
          getParentFolderRefAndSecurityTagReturnValue.head,
          getParentFolderRefAndSecurityTagReturnValue(1),
          getParentFolderRefAndSecurityTagReturnValue(2)
        )
      when(mockEntityClient.updateEntity(any[UpdateEntityRequest], any[String]))
        .thenReturn(updateEntityReturnValues)
      IO(mockEntityClient)
    }

    def verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows: Map[String, GetItemsResponse],
        numOfEntitiesByIdentifierInvocations: Int,
        numOfNodesFromEntityInvocations: Int,
        addEntityRequests: List[AddEntityRequest] = Nil,
        numOfAddIdentifierRequests: Int = 0,
        updateEntityRequests: List[UpdateEntityRequest] = Nil
    ): Unit = {
      val attributesValuesCaptor = getPrimaryKeysCaptor
      val tableNameCaptor = getTableNameCaptor
      verify(mockDynamoDBClient, times(1)).getItems[GetItemsResponse, PrimaryKey](
        attributesValuesCaptor.capture(),
        tableNameCaptor.capture()
      )(any[DynamoFormat[GetItemsResponse]], any[DynamoFormat[PrimaryKey]])
      attributesValuesCaptor.getValue.toArray.toList should be(
        folderIdsAndRows.map { case (ids, _) => PrimaryKey(ids) }
      )

      val entitiesByIdentifierIdentifierToGetCaptor = getIdentifierToGetCaptor
      val entitiesByIdentifierSecretNameCaptor = getEntitiesSecretNameCaptor

      verify(mockEntityClient, times(numOfEntitiesByIdentifierInvocations)).entitiesByIdentifier(
        entitiesByIdentifierIdentifierToGetCaptor.capture(),
        entitiesByIdentifierSecretNameCaptor.capture()
      )

      if (numOfEntitiesByIdentifierInvocations > 0) {
        val folderRows: Iterator[GetItemsResponse] = folderIdsAndRows.values.iterator

        entitiesByIdentifierIdentifierToGetCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfEntitiesByIdentifierInvocations)(Identifier("SourceId", folderRows.next().name))
        )

        entitiesByIdentifierSecretNameCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfEntitiesByIdentifierInvocations)("mockSecretName")
        )
      }

      val numOfAddEntityInvocations = addEntityRequests.length
      val addEntityEntitiesSecretNameCaptor = getEntitiesSecretNameCaptor
      val addEntityAddFolderRequestCaptor = getAddFolderRequestCaptor

      verify(mockEntityClient, times(numOfAddEntityInvocations)).addEntity(
        addEntityAddFolderRequestCaptor.capture(),
        addEntityEntitiesSecretNameCaptor.capture()
      )

      if (numOfAddEntityInvocations > 0) {
        addEntityAddFolderRequestCaptor.getAllValues.toArray.toList should be(addEntityRequests)
        addEntityEntitiesSecretNameCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfAddEntityInvocations)("mockSecretName")
        )
      }

      val addIdentifiersRefCaptor = getRefCaptor
      val addIdentifiersStructuralObjectCaptor = structuralObjectCaptor
      val addIdentifiersIdentifiersToAddCaptor = identifiersToAddCaptor
      val addIdentifiersSecretNameCaptor = getEntitiesSecretNameCaptor

      verify(mockEntityClient, times(numOfAddIdentifierRequests)).addIdentifiersForEntity(
        addIdentifiersRefCaptor.capture(),
        addIdentifiersStructuralObjectCaptor.capture(),
        addIdentifiersIdentifiersToAddCaptor.capture(),
        addIdentifiersSecretNameCaptor.capture()
      )

      if (numOfAddIdentifierRequests > 0) {
        addIdentifiersRefCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfAddIdentifierRequests)(addEntityReturnValue.unsafeRunSync())
        )

        addIdentifiersStructuralObjectCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfAddIdentifierRequests)("structural-objects")
        )

        addIdentifiersIdentifiersToAddCaptor.getAllValues.toArray.toList should be(
          addEntityRequests.map { addEntityRequest =>
            val folderName = addEntityRequest.title.get
            List(Identifier("SourceId", folderName), Identifier("Code", folderName))
          }
        )

        addIdentifiersSecretNameCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfAddIdentifierRequests)("mockSecretName")
        )
      }

      val nodesFromEntityRefCaptor = getRefCaptor
      val nodesFromEntityStructuralObjectCaptor = structuralObjectCaptor
      val nodesFromEntityChildNodesToAddCaptor = childNodesCaptor
      val nodesFromEntitySecretNameCaptor = getEntitiesSecretNameCaptor

      verify(mockEntityClient, times(numOfNodesFromEntityInvocations)).nodesFromEntity(
        nodesFromEntityRefCaptor.capture(),
        nodesFromEntityStructuralObjectCaptor.capture(),
        nodesFromEntityChildNodesToAddCaptor.capture(),
        nodesFromEntitySecretNameCaptor.capture()
      )

      if (numOfNodesFromEntityInvocations > 0) {
        val refsOfFoldersThatExistInPreservica = entitiesWithSourceIdReturnValue.collect {
          case responseIo if responseIo.unsafeRunSync().headOption.nonEmpty => responseIo.unsafeRunSync().head.ref
        }
        nodesFromEntityRefCaptor.getAllValues.toArray.toList should be(refsOfFoldersThatExistInPreservica)

        nodesFromEntityStructuralObjectCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfNodesFromEntityInvocations)("structural-objects")
        )

        nodesFromEntityChildNodesToAddCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfNodesFromEntityInvocations)(List("ParentRef", "SecurityTag"))
        )

        nodesFromEntitySecretNameCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfNodesFromEntityInvocations)("mockSecretName")
        )
      }

      val numOfUpdateEntityInvocations = updateEntityRequests.length
      val updateEntityUpdateFolderRequestCaptor = getUpdateFolderRequestCaptor
      val updateEntitySecretNameCaptor = getEntitiesSecretNameCaptor

      verify(mockEntityClient, times(numOfUpdateEntityInvocations)).updateEntity(
        updateEntityUpdateFolderRequestCaptor.capture(),
        updateEntitySecretNameCaptor.capture()
      )

      if (numOfUpdateEntityInvocations > 0) {
        updateEntityUpdateFolderRequestCaptor.getAllValues.toArray.toList should be(updateEntityRequests)

        updateEntitySecretNameCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfUpdateEntityInvocations)("mockSecretName")
        )
      }

      ()
    }
  }
}

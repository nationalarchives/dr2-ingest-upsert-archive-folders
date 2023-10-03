package uk.gov.nationalarchives.testUtils

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.Encoder
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar.{mock, times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor6}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scanamo.DynamoFormat
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.Lambda.{EntityWithUpdateEntityRequest, GetItemsResponse, PartitionKey}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, Identifier}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.{AddEntityRequest, Open, StructuralObject, UpdateEntityRequest}
import uk.gov.nationalarchives.{DADynamoDBClient, DAEventBridgeClient, Lambda}

import scala.jdk.CollectionConverters._
import java.util.UUID
import scala.collection.immutable.ListMap

class ExternalServicesTestUtils
    extends AnyFlatSpec
    with BeforeAndAfterEach
    with BeforeAndAfterAll
    with TableDrivenPropertyChecks {

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
        Some(StructuralObject),
        UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"),
        Some("mock title_1"),
        Some("mock description_1"),
        deleted = false,
        Some(StructuralObject.entityPath),
        Some(Open),
        Some(UUID.fromString("562530e3-3b6e-435a-8b56-1d3ad4868a9a"))
      )
    ),
    1 -> Seq(
      Entity(
        Some(StructuralObject),
        UUID.fromString("a2d39ea3-6216-4f93-b078-62c7896b174c"),
        Some("mock title_1_1"),
        Some("mock description_1_1"),
        deleted = false,
        Some(StructuralObject.entityPath),
        Some(Open),
        Some(UUID.fromString("d7879799-a7de-4aa6-8c7b-afced66a6c50"))
      )
    ),
    2 -> Seq(
      Entity(
        Some(StructuralObject),
        UUID.fromString("9dfc40be-5f44-4fa1-9c25-fbe03dd3f539"),
        Some("mock title_1_1_1"),
        Some("mock description_1_1_1"),
        deleted = false,
        Some(StructuralObject.entityPath),
        Some(Open),
        Some(UUID.fromString("a2d39ea3-6216-4f93-b078-62c7896b174c"))
      )
    )
  )

  val defaultEntitiesWithSourceIdReturnValues: List[IO[Seq[Entity]]] =
    List(IO(structuralObjects(0)), IO(structuralObjects(1)), IO(structuralObjects(2)))

  val missingTitleInDbScenarios
      : TableFor6[String, Option[String], Option[String], Option[String], Option[String], String] = Table(
    (
      "Test",
      "Title from DB",
      "Title from Preservica",
      "Description from DB",
      "Description from Preservica",
      "Test result"
    ),
    (
      "title not found in DB, title was found in Preservica but no description updates necessary",
      None,
      Some(""),
      Some("mock description_1"),
      Some("mock description_1"),
      "make no calls to 'updateEntity'"
    ),
    (
      "title not found in DB, title was found in Preservica but description needs to be updated",
      None,
      Some(""),
      Some("mock description_1"),
      Some("mock description_old_1"),
      "call to updateEntity to update description, using existing Entity title as 'title'"
    ),
    (
      "title and description not found in DB, title and description found in Preservica",
      None,
      Some(""),
      None,
      Some(""),
      "make no calls to 'updateEntity'"
    ),
    (
      "title and description not found in DB, title found in Preservica but not description",
      None,
      Some(""),
      None,
      None,
      "make no calls to 'updateEntity'"
    )
  )

  val missingDescriptionInDbScenarios
      : TableFor6[String, Option[String], Option[String], Option[String], Option[String], String] = Table(
    (
      "Test",
      "Title from DB",
      "Title from Preservica",
      "Description from DB",
      "Description from Preservica",
      "Test result"
    ),
    (
      "description not found in DB, description was found in Preservica but no title updates necessary",
      Some("mock title_1"),
      Some("mock title_1"),
      None,
      Some(""),
      "make no calls to 'updateEntity'"
    ),
    (
      "description not found in DB, description was found in Preservica but title needs to be updated",
      Some("mock title_1"),
      Some("mock title_old_1"),
      None,
      Some(""),
      "call to updateEntity to update title, using None as the 'description'"
    ),
    (
      "description not found in DB, description not found in Preservica but no title updates necessary",
      Some("mock title_1"),
      Some("mock title_1"),
      None,
      None,
      "make no calls to 'updateEntity'"
    ),
    (
      "description not found in DB, description not found in Preservica but title needs to be updated",
      Some("mock title_1"),
      Some("mock title_old_1"),
      None,
      None,
      "call to updateEntity to update title, using None as the 'description'"
    )
  )

  case class MockLambda(
      getAttributeValuesReturnValue: IO[List[GetItemsResponse]],
      entitiesWithSourceIdReturnValue: List[IO[Seq[Entity]]] = defaultEntitiesWithSourceIdReturnValues,
      addEntityReturnValues: List[IO[UUID]] = List(
        IO(structuralObjects(0).head.ref),
        IO(structuralObjects(1).head.ref),
        IO(structuralObjects(2).head.ref)
      ),
      addIdentifierReturnValue: IO[String] = IO("The Identifier was added"),
      updateEntityReturnValues: IO[String] = IO("Entity was updated")
  ) extends Lambda() {
    val testEventBridgeClient: DAEventBridgeClient[IO] = mock[DAEventBridgeClient[IO]]
    val eventBridgeMessageCaptors: ArgumentCaptor[Detail] = ArgumentCaptor.forClass(classOf[Detail])
    when(
      testEventBridgeClient.publishEventToEventBridge[Detail](
        any[String],
        any[String],
        eventBridgeMessageCaptors.capture()
      )(any[Encoder[Detail]])
    ).thenReturn(IO(PutEventsResponse.builder.build))
    override lazy val eventBridgeClient: DAEventBridgeClient[IO] = testEventBridgeClient
    val apiUrlCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    def getIdentifierToGetCaptor: ArgumentCaptor[Identifier] = ArgumentCaptor.forClass(classOf[Identifier])
    def getAddFolderRequestCaptor: ArgumentCaptor[AddEntityRequest] = ArgumentCaptor.forClass(classOf[AddEntityRequest])
    def getRefCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    def structuralObjectCaptor: ArgumentCaptor[StructuralObject.type] =
      ArgumentCaptor.forClass(classOf[StructuralObject.type])
    def identifiersToAddCaptor: ArgumentCaptor[Identifier] = ArgumentCaptor.forClass(classOf[Identifier])
    def getUpdateFolderRequestCaptor: ArgumentCaptor[UpdateEntityRequest] =
      ArgumentCaptor.forClass(classOf[UpdateEntityRequest])

    val entityCaptor: ArgumentCaptor[Entity] = ArgumentCaptor.forClass(classOf[Entity])

    def getPartitionKeysCaptor: ArgumentCaptor[List[PartitionKey]] =
      ArgumentCaptor.forClass(classOf[List[PartitionKey]])
    def getTableNameCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    val mockEntityClient: EntityClient[IO, Fs2Streams[IO]] = mock[EntityClient[IO, Fs2Streams[IO]]]
    val mockDynamoDBClient: DADynamoDBClient[IO] = mock[DADynamoDBClient[IO]]

    override val dADynamoDBClient: DADynamoDBClient[IO] = {
      when(
        mockDynamoDBClient.getItems[GetItemsResponse, PartitionKey](any[List[PartitionKey]], any[String])(
          any[DynamoFormat[GetItemsResponse]],
          any[DynamoFormat[PartitionKey]]
        )
      ).thenReturn(
        getAttributeValuesReturnValue
      )

      mockDynamoDBClient
    }

    override lazy val entitiesClientIO: IO[EntityClient[IO, Fs2Streams[IO]]] = {
      when(mockEntityClient.entitiesByIdentifier(any[Identifier]))
        .thenReturn(
          entitiesWithSourceIdReturnValue.head,
          entitiesWithSourceIdReturnValue(1),
          entitiesWithSourceIdReturnValue(2)
        )
      when(mockEntityClient.addEntity(any[AddEntityRequest])).thenReturn(
        addEntityReturnValues.head,
        addEntityReturnValues.lift(1).getOrElse(IO(UUID.randomUUID())),
        addEntityReturnValues.lift(2).getOrElse(IO(UUID.randomUUID()))
      )
      when(
        mockEntityClient.addIdentifierForEntity(
          any[UUID],
          any[StructuralObject.type],
          any[Identifier]
        )
      )
        .thenReturn(addIdentifierReturnValue)
      when(mockEntityClient.updateEntity(any[UpdateEntityRequest]))
        .thenReturn(updateEntityReturnValues)
      IO(mockEntityClient)
    }

    def verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows: Map[String, GetItemsResponse],
        numOfEntitiesByIdentifierInvocations: Int,
        addEntityRequests: List[AddEntityRequest] = Nil,
        numOfAddIdentifierRequests: Int = 0,
        updateEntityRequests: List[EntityWithUpdateEntityRequest] = Nil
    ): Unit = {
      val attributesValuesCaptor = getPartitionKeysCaptor
      val tableNameCaptor = getTableNameCaptor
      verify(mockDynamoDBClient, times(1)).getItems[GetItemsResponse, PartitionKey](
        attributesValuesCaptor.capture(),
        tableNameCaptor.capture()
      )(any[DynamoFormat[GetItemsResponse]], any[DynamoFormat[PartitionKey]])
      attributesValuesCaptor.getValue.toArray.toList should be(
        folderIdsAndRows.map { case (ids, _) => PartitionKey(ids) }
      )

      val entitiesByIdentifierIdentifierToGetCaptor = getIdentifierToGetCaptor

      verify(mockEntityClient, times(numOfEntitiesByIdentifierInvocations)).entitiesByIdentifier(
        entitiesByIdentifierIdentifierToGetCaptor.capture()
      )

      if (numOfEntitiesByIdentifierInvocations > 0) {
        val folderRows: Iterator[GetItemsResponse] = folderIdsAndRows.values.iterator

        entitiesByIdentifierIdentifierToGetCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfEntitiesByIdentifierInvocations)(Identifier("SourceID", folderRows.next().name))
        )
      }

      val numOfAddEntityInvocations = addEntityRequests.length
      val addEntityAddFolderRequestCaptor = getAddFolderRequestCaptor

      verify(mockEntityClient, times(numOfAddEntityInvocations)).addEntity(
        addEntityAddFolderRequestCaptor.capture()
      )

      if (numOfAddEntityInvocations > 0) {
        addEntityAddFolderRequestCaptor.getAllValues.toArray.toList should be(addEntityRequests)
      }

      val addIdentifiersRefCaptor = getRefCaptor
      val addIdentifiersStructuralObjectCaptor = structuralObjectCaptor
      val addIdentifiersIdentifiersToAddCaptor = identifiersToAddCaptor

      verify(mockEntityClient, times(numOfAddIdentifierRequests)).addIdentifierForEntity(
        addIdentifiersRefCaptor.capture(),
        addIdentifiersStructuralObjectCaptor.capture(),
        addIdentifiersIdentifiersToAddCaptor.capture()
      )

      if (numOfAddIdentifierRequests > 0) {
        val numOfAddIdentifierRequestsPerEntity = 2
        addIdentifiersRefCaptor.getAllValues.toArray.toList should be(
          addEntityReturnValues.flatMap { addEntityReturnValue =>
            List.fill(numOfAddIdentifierRequestsPerEntity)(addEntityReturnValue.unsafeRunSync())
          }
        )

        addIdentifiersStructuralObjectCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfAddIdentifierRequests)(StructuralObject)
        )

        addIdentifiersIdentifiersToAddCaptor.getAllValues.toArray.toList should be(
          addEntityRequests.flatMap { addEntityRequest =>
            val folderName = addEntityRequest.title
            List(Identifier("SourceID", folderName), Identifier("Code", folderName))
          }
        )
      }

      val numOfUpdateEntityInvocations = updateEntityRequests.length
      val updateEntityUpdateFolderRequestCaptor = getUpdateFolderRequestCaptor

      verify(mockEntityClient, times(numOfUpdateEntityInvocations)).updateEntity(
        updateEntityUpdateFolderRequestCaptor.capture()
      )

      if (numOfUpdateEntityInvocations > 0) {
        updateEntityUpdateFolderRequestCaptor.getAllValues.toArray.toList should be(
          updateEntityRequests.map(_.updateEntityRequest)
        )
      }

      val sentMessages = eventBridgeMessageCaptors.getAllValues.asScala.map(_.slackMessage)

      if (updateEntityReturnValues.attempt.unsafeRunSync().isRight) {
        sentMessages.length should equal(updateEntityRequests.size)
        updateEntityRequests.foreach { entityAndUpdateRequest =>
          val updateRequest = entityAndUpdateRequest.updateEntityRequest
          val entity = entityAndUpdateRequest.entity
          val oldTitle = entity.title.getOrElse("")
          val newTitle = updateRequest.title
          val oldDescription = entity.description.getOrElse("")
          val newDescription = updateRequest.descriptionToChange.getOrElse("")
          val expectedMessage = s":preservica: Entity ${updateRequest.ref} has been updated\n" +
            s"*Old title*: $oldTitle\n*New title*: $newTitle\n*Old description*: $oldDescription\n*New description*: $newDescription\n"
          sentMessages.count(_ == expectedMessage) should equal(1)
        }
      }
      ()
    }
  }
}

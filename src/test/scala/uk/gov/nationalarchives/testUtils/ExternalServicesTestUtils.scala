package uk.gov.nationalarchives.testUtils

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.Encoder
import org.mockito.ArgumentCaptor
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor6}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatestplus.mockito.MockitoSugar.mock
import org.scanamo.DynamoFormat
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.Lambda.EntityWithUpdateEntityRequest
import uk.gov.nationalarchives.DynamoFormatters.*
import uk.gov.nationalarchives.dp.client.Entities.{Entity, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.EntityType.*
import uk.gov.nationalarchives.dp.client.EntityClient.SecurityTag.*
import uk.gov.nationalarchives.dp.client.EntityClient.{AddEntityRequest, EntityType, UpdateEntityRequest}
import uk.gov.nationalarchives.{DADynamoDBClient, DAEventBridgeClient, Lambda}

import scala.jdk.CollectionConverters.*
import java.util.UUID
import scala.collection.immutable.ListMap

class ExternalServicesTestUtils extends AnyFlatSpec with BeforeAndAfterEach with BeforeAndAfterAll with TableDrivenPropertyChecks:

  val folderIdsAndRows: ListMap[UUID, ArchiveFolderDynamoTable] = ListMap(
    UUID.fromString("f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176") ->
      ArchiveFolderDynamoTable(
        "batchId",
        UUID.fromString("f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176"),
        None,
        "mock title_1",
        ArchiveFolder,
        Some("mock title_1"),
        Some("mock description_1"),
        List(Identifier("Code", "code"))
      ),
    UUID.fromString("e88e433a-1f3e-48c5-b15f-234c0e663c27") -> ArchiveFolderDynamoTable(
      "batchId",
      UUID.fromString("e88e433a-1f3e-48c5-b15f-234c0e663c27"),
      Some("f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176"),
      "mock title_1_1",
      ArchiveFolder,
      Some("mock title_1_1"),
      Some("mock description_1_1"),
      List(Identifier("Code", "code"))
    ),
    UUID.fromString("93f5a200-9ee7-423d-827c-aad823182ad2") -> ArchiveFolderDynamoTable(
      "batchId",
      UUID.fromString("93f5a200-9ee7-423d-827c-aad823182ad2"),
      Some("f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176/e88e433a-1f3e-48c5-b15f-234c0e663c27"),
      "mock title_1_1_1",
      ArchiveFolder,
      Some("mock title_1_1_1"),
      Some("mock description_1_1_1"),
      List(Identifier("Code", "code"))
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
        Some(open),
        None
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
        Some(open),
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
        Some(open),
        Some(UUID.fromString("a2d39ea3-6216-4f93-b078-62c7896b174c"))
      )
    )
  )

  val defaultEntitiesWithSourceIdReturnValues: List[IO[Seq[Entity]]] =
    List(IO(structuralObjects(0)), IO(structuralObjects(1)), IO(structuralObjects(2)))

  val defaultIdentifiersReturnValue: IO[Seq[IdentifierResponse]] = IO(Seq(IdentifierResponse("id", "Code", "code")))

  val missingTitleInDbScenarios: TableFor6[String, Option[String], Option[String], Option[String], Option[String], String] = Table(
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

  val missingDescriptionInDbScenarios: TableFor6[String, Option[String], Option[String], Option[String], Option[String], String] = Table(
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

  private val singleIdentifier1: List[Identifier] = List(Identifier("Test1", "Value1"))
  private val singleIdentifier1DifferentValue: List[Identifier] = List(Identifier("Test1", "Value2"))
  private val singleIdentifier2: List[Identifier] = List(Identifier("Test2", "Value2"))
  private val multipleIdentifiers: List[Identifier] = singleIdentifier1 ++ singleIdentifier2
  private val singleIdentifier3: List[Identifier] = List(Identifier("Test2", "Value3"))

  val identifierScenarios: TableFor6[List[Identifier], List[Identifier], List[Identifier], List[Identifier], String, String] = Table(
    ("identifierFromDynamo", "identifierFromPreservica", "addIdentifierRequest", "updateIdentifierRequest", "addResult", "updateResult"),
    (singleIdentifier1, singleIdentifier1, Nil, Nil, addIdentifiersDescription(Nil), updateIdentifiersDescription(Nil)),
    (
      singleIdentifier1DifferentValue,
      singleIdentifier1,
      Nil,
      singleIdentifier1DifferentValue,
      addIdentifiersDescription(Nil),
      updateIdentifiersDescription(singleIdentifier1)
    ),
    (singleIdentifier1, singleIdentifier2, singleIdentifier1, Nil, addIdentifiersDescription(singleIdentifier1), updateIdentifiersDescription(Nil)),
    (
      multipleIdentifiers,
      singleIdentifier3,
      singleIdentifier1,
      singleIdentifier2,
      addIdentifiersDescription(singleIdentifier1),
      updateIdentifiersDescription(singleIdentifier2)
    )
  )

  private def addIdentifiersDescription(identifiers: List[Identifier]) = identifiersTestDescription(identifiers, "add")
  private def updateIdentifiersDescription(identifiers: List[Identifier]) = identifiersTestDescription(identifiers, "update")
  private def identifiersTestDescription(identifiers: List[Identifier], operation: String) =
    if identifiers.isEmpty then s"not $operation any identifiers"
    else
      val identifiersString = identifiers.map(i => s"${i.identifierName}=${i.value}").mkString(" ")
      s"add $identifiersString"

  case class MockLambda(
      getAttributeValuesReturnValue: IO[List[ArchiveFolderDynamoTable]],
      entitiesWithSourceIdReturnValue: List[IO[Seq[Entity]]] = defaultEntitiesWithSourceIdReturnValues,
      addEntityReturnValues: List[IO[UUID]] = List(
        IO(structuralObjects(0).head.ref),
        IO(structuralObjects(1).head.ref),
        IO(structuralObjects(2).head.ref)
      ),
      addIdentifierReturnValue: IO[String] = IO("The Identifier was added"),
      updateEntityReturnValues: IO[String] = IO("Entity was updated"),
      getIdentifiersForEntityReturnValues: IO[Seq[IdentifierResponse]] = defaultIdentifiersReturnValue
  ) extends Lambda():
    val testEventBridgeClient: DAEventBridgeClient[IO] = mock[DAEventBridgeClient[IO]]
    val eventBridgeMessageCaptors: ArgumentCaptor[Detail] = ArgumentCaptor.forClass(classOf[Detail])
    when(
      testEventBridgeClient.publishEventToEventBridge[Detail](
        any[String],
        any[String],
        eventBridgeMessageCaptors.capture()
      )(using any[Encoder[Detail]])
    ).thenReturn(IO(PutEventsResponse.builder.build))
    override lazy val eventBridgeClient: DAEventBridgeClient[IO] = testEventBridgeClient
    val apiUrlCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])
    def getIdentifierToGetCaptor: ArgumentCaptor[Identifier] = ArgumentCaptor.forClass(classOf[Identifier])
    def getAddFolderRequestCaptor: ArgumentCaptor[AddEntityRequest] = ArgumentCaptor.forClass(classOf[AddEntityRequest])
    def getRefCaptor: ArgumentCaptor[UUID] = ArgumentCaptor.forClass(classOf[UUID])
    def structuralObjectCaptor: ArgumentCaptor[StructuralObject.type] =
      ArgumentCaptor.captor()
    def identifiersToAddCaptor: ArgumentCaptor[Identifier] = ArgumentCaptor.forClass(classOf[Identifier])
    def getUpdateFolderRequestCaptor: ArgumentCaptor[UpdateEntityRequest] =
      ArgumentCaptor.forClass(classOf[UpdateEntityRequest])

    val entityCaptor: ArgumentCaptor[Entity] = ArgumentCaptor.forClass(classOf[Entity])

    def getPartitionKeysCaptor: ArgumentCaptor[List[PartitionKey]] =
      ArgumentCaptor.forClass(classOf[List[PartitionKey]])
    def getTableNameCaptor: ArgumentCaptor[String] = ArgumentCaptor.forClass(classOf[String])

    val mockEntityClient: EntityClient[IO, Fs2Streams[IO]] = mock[EntityClient[IO, Fs2Streams[IO]]]
    val mockDynamoDBClient: DADynamoDBClient[IO] = mock[DADynamoDBClient[IO]]

    override val dADynamoDBClient: DADynamoDBClient[IO] =
      when(
        mockDynamoDBClient.getItems[ArchiveFolderDynamoTable, PartitionKey](any[List[PartitionKey]], any[String])(using
          any[DynamoFormat[ArchiveFolderDynamoTable]],
          any[DynamoFormat[PartitionKey]]
        )
      ).thenReturn(
        getAttributeValuesReturnValue
      )

      mockDynamoDBClient

    override lazy val entitiesClientIO: IO[EntityClient[IO, Fs2Streams[IO]]] =
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
      when(mockEntityClient.getEntityIdentifiers(any[Entity]))
        .thenReturn(getIdentifiersForEntityReturnValues)
      when(mockEntityClient.updateEntityIdentifiers(any[Entity], any[Seq[IdentifierResponse]]))
        .thenReturn(getIdentifiersForEntityReturnValues)
      IO(mockEntityClient)

    def verifyInvocationsAndArgumentsPassed(
        folderIdsAndRows: Map[UUID, ArchiveFolderDynamoTable],
        numOfEntitiesByIdentifierInvocations: Int,
        addEntityRequests: List[AddEntityRequest] = Nil,
        numOfAddIdentifierRequests: Int = 0,
        updateEntityRequests: List[EntityWithUpdateEntityRequest] = Nil
    ): Unit =
      val attributesValuesCaptor = getPartitionKeysCaptor
      val tableNameCaptor = getTableNameCaptor
      verify(mockDynamoDBClient, times(1)).getItems[ArchiveFolderDynamoTable, PartitionKey](
        attributesValuesCaptor.capture(),
        tableNameCaptor.capture()
      )(using any[DynamoFormat[ArchiveFolderDynamoTable]], any[DynamoFormat[PartitionKey]])
      attributesValuesCaptor.getValue.toArray.toList should be(
        folderIdsAndRows.map { case (ids, _) => PartitionKey(ids) }
      )

      val entitiesByIdentifierIdentifierToGetCaptor = getIdentifierToGetCaptor

      verify(mockEntityClient, times(numOfEntitiesByIdentifierInvocations)).entitiesByIdentifier(
        entitiesByIdentifierIdentifierToGetCaptor.capture()
      )

      if numOfEntitiesByIdentifierInvocations > 0 then
        val folderRows: Iterator[ArchiveFolderDynamoTable] = folderIdsAndRows.values.iterator

        entitiesByIdentifierIdentifierToGetCaptor.getAllValues.toArray.toList should be(
          List.fill(numOfEntitiesByIdentifierInvocations)(Identifier("SourceID", folderRows.next().name))
        )

      val numOfAddEntityInvocations = addEntityRequests.length
      val addEntityAddFolderRequestCaptor = getAddFolderRequestCaptor

      verify(mockEntityClient, times(numOfAddEntityInvocations)).addEntity(
        addEntityAddFolderRequestCaptor.capture()
      )

      if numOfAddEntityInvocations > 0 then addEntityAddFolderRequestCaptor.getAllValues.toArray.toList should be(addEntityRequests)

      val addIdentifiersRefCaptor = getRefCaptor
      val addIdentifiersStructuralObjectCaptor = structuralObjectCaptor
      val addIdentifiersIdentifiersToAddCaptor = identifiersToAddCaptor

      verify(mockEntityClient, times(numOfAddIdentifierRequests)).addIdentifierForEntity(
        addIdentifiersRefCaptor.capture(),
        addIdentifiersStructuralObjectCaptor.capture(),
        addIdentifiersIdentifiersToAddCaptor.capture()
      )

      if numOfAddIdentifierRequests > 0 then
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
            List(Identifier("SourceID", folderName), Identifier("Code", "code"))
          }
        )

      val numOfUpdateEntityInvocations = updateEntityRequests.length
      val updateEntityUpdateFolderRequestCaptor = getUpdateFolderRequestCaptor

      verify(mockEntityClient, times(numOfUpdateEntityInvocations)).updateEntity(
        updateEntityUpdateFolderRequestCaptor.capture()
      )

      val sentMessages = eventBridgeMessageCaptors.getAllValues.asScala.map(_.slackMessage)

      if numOfUpdateEntityInvocations > 0 then
        updateEntityUpdateFolderRequestCaptor.getAllValues.toArray.toList should be(
          updateEntityRequests.map(_.updateEntityRequest)
        )
      else sentMessages.length should equal(0)

      if updateEntityReturnValues.attempt.unsafeRunSync().isRight then
        sentMessages.length should equal(updateEntityRequests.size)
        updateEntityRequests.foreach { entityAndUpdateRequest =>
          val updateRequest = entityAndUpdateRequest.updateEntityRequest
          val entity = entityAndUpdateRequest.entity
          val oldTitle = entity.title.getOrElse("")
          val newTitle = updateRequest.title

          val oldDescription = entity.description.getOrElse("")
          val newDescription = updateRequest.descriptionToChange.getOrElse("")
          val entityTypeShort = entity.entityType.get.entityTypeShort
          val url = "http://localhost:9001/explorer/explorer.html#properties"
          val messageFirstLine =
            s":preservica: Entity <$url:$entityTypeShort&${entity.ref}|${entity.ref}> has been updated\n"
          val expectedMessage =
            if oldTitle != newTitle && updateRequest.descriptionToChange.isEmpty then
              messageFirstLine +
                s"*Old title*: $oldTitle\n*New title*: $newTitle"
            else if oldTitle == newTitle && updateRequest.descriptionToChange.isDefined then
              messageFirstLine +
                s"*Old description*: $oldDescription\n*New description*: $newDescription"
            else
              messageFirstLine +
                s"*Old title*: $oldTitle\n*New title*: $newTitle\n*Old description*: $oldDescription\n*New description*: $newDescription"
          sentMessages.count(_ == expectedMessage) should equal(1)
        }
      else sentMessages.length should equal(0)
      ()

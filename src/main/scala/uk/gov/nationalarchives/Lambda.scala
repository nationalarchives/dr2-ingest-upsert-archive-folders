package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse
import sttp.capabilities.fs2.Fs2Streams
import io.circe.generic.auto._
import uk.gov.nationalarchives.DynamoFormatters._
import uk.gov.nationalarchives.Lambda.{Config, EntityWithUpdateEntityRequest, FullFolderInfo, StepFnInput, IdentifierToUpdate}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.{AddEntityRequest, Closed, Open, SecurityTag, StructuralObject, UpdateEntityRequest}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import upickle.default

import java.io.{InputStream, OutputStream}
import java.util.UUID
import scala.io.Source

class Lambda extends RequestStreamHandler {
  lazy val eventBridgeClient: DAEventBridgeClient[IO] = DAEventBridgeClient[IO]()

  lazy val entitiesClientIO: IO[EntityClient[IO, Fs2Streams[IO]]] = configIo.flatMap { config =>
    Fs2Client.entityClient(config.apiUrl, config.secretName)
  }
  val dADynamoDBClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  private val structuralObject = StructuralObject
  private val sourceId = "SourceID"

  private val configIo: IO[Config] = ConfigSource.default.loadF[IO, Config]()
  private implicit val secretRW: default.ReadWriter[StepFnInput] = default.macroRW[StepFnInput]
  case class Detail(slackMessage: String)

  private def sendToSlack(slackMessage: String): IO[PutEventsResponse] =
    eventBridgeClient.publishEventToEventBridge(getClass.getName, "DR2DevMessage", Detail(slackMessage))

  override def handleRequest(input: InputStream, output: OutputStream, context: Context): Unit = {
    val rawInput: String = Source.fromInputStream(input).mkString
    val stepFnInput = default.read[StepFnInput](rawInput)

    val folderIdPartitionKeysAndValues: List[PartitionKey] =
      stepFnInput.archiveHierarchyFolders.map(UUID.fromString).map(PartitionKey)

    for {
      config <- configIo
      folderRowsSortedByParentPath <- getFolderRowsSortedByParentPath(
        folderIdPartitionKeysAndValues,
        config.archiveFolderTableName
      )

      _ <- checkNumOfParentPathSlashesPerFolderIncrease(folderRowsSortedByParentPath)
      _ <- checkEachParentPathMatchesFolderBeforeIt(folderRowsSortedByParentPath)

      entitiesClient <- entitiesClientIO
      secretName = config.secretName

      potentialEntitiesWithSourceId <- getEntitiesByIdentifier(folderRowsSortedByParentPath, entitiesClient)
      folderIdAndInfo <-
        verifyOnlyOneEntityReturnedAndGetFullFolderInfo(folderRowsSortedByParentPath.zip(potentialEntitiesWithSourceId))
      folderInfoWithExpectedParentRef <- getExpectedParentRefForEachFolder(folderIdAndInfo)

      (folderInfoOfEntitiesThatDoNotExist, folderInfoOfEntitiesThatExist) = folderInfoWithExpectedParentRef.partition(
        _.entity.isEmpty
      )
      _ <- createFolders(folderInfoOfEntitiesThatDoNotExist, entitiesClient, secretName)
      _ <- verifyEntitiesAreStructuralObjects(folderInfoOfEntitiesThatExist)

      folderInfoOfEntitiesThatExistWithSecurityTags <-
        verifyExpectedParentFolderMatchesFolderFromApiAndGetSecurityTag(folderInfoOfEntitiesThatExist)
      folderUpdateRequests = findOnlyFoldersThatNeedUpdatingAndCreateRequests(
        folderInfoOfEntitiesThatExistWithSecurityTags
      )

      _ <- folderInfoOfEntitiesThatExistWithSecurityTags.map { fi =>
        val identifiersFromDynamo = fi.folderRow.identifiers
        val entity = fi.entity.get
        for {
          identifiersFromPreservica <- entitiesClient.getEntityIdentifiers(entity)
          identifiersToUpdate <- findIdentifiersToUpdate(identifiersFromDynamo, identifiersFromPreservica)
          identifiersToAdd <- findIdentifiersToAdd(identifiersFromDynamo, identifiersFromPreservica)
          _ <- identifiersToAdd.map { id =>
            entitiesClient.addIdentifierForEntity(entity.ref, entity.entityType.getOrElse(StructuralObject), id)
          }.sequence
          updatedIdentifier = identifiersToUpdate.map(_.newIdentifier)
          _ <-
            if (updatedIdentifier.nonEmpty)
              entitiesClient.updateEntityIdentifiers(entity, identifiersToUpdate.map(_.newIdentifier))
            else IO.unit
          updatedSlackMessage <- generateIdentifierSlackMessage(
            config.apiUrl,
            entity,
            identifiersToUpdate,
            identifiersToAdd
          )
          _ <- updatedSlackMessage.map(sendToSlack).sequence
        } yield ()
      }.sequence
      _ <- folderUpdateRequests.map { folderUpdateRequest =>
        val message = generateTitleDescriptionSlackMessage(config.apiUrl, folderUpdateRequest)
        for {
          _ <- entitiesClient.updateEntity(folderUpdateRequest.updateEntityRequest)
          _ <- sendToSlack(message)
        } yield ()
      }.sequence
    } yield ()
  }.unsafeRunSync()

  private def generateIdentifierSlackMessage(
      preservicaUrl: String,
      entity: Entity,
      updatedIdentifier: Seq[IdentifierToUpdate],
      addedIdentifiers: Seq[Identifier]
  ): IO[Option[String]] = IO {
    if (updatedIdentifier.isEmpty && addedIdentifiers.isEmpty) {
      None
    } else
      Option {
        val firstLine = generateSlackMessageFirstLine(preservicaUrl, entity)
        val potentialUpdatedHeader = updatedIdentifier.headOption
          .map(_ => "The following identifiers have been updated\n")
          .getOrElse("")
        val updatedIdentifierMessage = updatedIdentifier
          .map { ui =>
            s"""
                     |*Old value* ${ui.oldIdentifier.identifierName}: ${ui.oldIdentifier.value}
                     |*New value* ${ui.newIdentifier.identifierName}: ${ui.newIdentifier.value}
                     |
                     |""".stripMargin
          }
          .mkString("")
        val potentialAddIdentifiersHeader = addedIdentifiers.headOption.map(_ => "The following identifiers have been added\n").getOrElse("")
        val addedIdentifiersMessage = addedIdentifiers
          .map { ai =>
            s"""
                    |${ai.identifierName}: ${ai.value}
                    |""".stripMargin
          }
          .mkString("")
        firstLine + potentialUpdatedHeader + updatedIdentifierMessage + potentialAddIdentifiersHeader + addedIdentifiersMessage
      }

  }

  private def generateTitleDescriptionSlackMessage(
      preservicaUrl: String,
      folderUpdateRequest: EntityWithUpdateEntityRequest
  ): String = {
    val entity: Entity = folderUpdateRequest.entity
    val firstLine: String = generateSlackMessageFirstLine(preservicaUrl, entity)
    val updateEntityRequest = folderUpdateRequest.updateEntityRequest

    def generateMessage(name: String, oldString: String, newString: String) =
      s"""*Old $name*: $oldString
         |*New $name*: $newString""".stripMargin

    val titleUpdates = Option.when(entity.title.getOrElse("") != updateEntityRequest.title)(
      generateMessage("title", entity.title.getOrElse(""), updateEntityRequest.title)
    )
    val descriptionUpdates = Option.when(updateEntityRequest.descriptionToChange.isDefined)(
      generateMessage("description", entity.description.getOrElse(""), updateEntityRequest.descriptionToChange.get)
    )

    firstLine ++ List(titleUpdates, descriptionUpdates).flatten.mkString("\n")
  }

  private def generateSlackMessageFirstLine(preservicaUrl: String, entity: Entity) = {
    val entityTypeShort = entity.entityType
      .map(_.entityTypeShort)
      .getOrElse("IO") // We need a default and Preservica don't validate the entity type in the url
    val entityUrl = s"$preservicaUrl/explorer/explorer.html#properties:$entityTypeShort&${entity.ref}"
    s""":preservica: Entity <$entityUrl|${entity.ref}> has been updated
                                  |""".stripMargin
  }

  private def getFolderRowsSortedByParentPath(
      folderIdPartitionKeysAndValues: List[PartitionKey],
      archiveFolderTableName: String
  ): IO[List[DynamoTable]] = {
    val getItemsResponse: IO[List[DynamoTable]] =
      dADynamoDBClient.getItems[DynamoTable, PartitionKey](
        folderIdPartitionKeysAndValues,
        archiveFolderTableName
      )

    getItemsResponse.map(_.sortBy(folderRow => folderRow.parentPath))
  }

  private def checkNumOfParentPathSlashesPerFolderIncrease(
      folderRowsSortedByParentPath: List[DynamoTable]
  ): IO[List[Int]] = {
    val numberOfSlashesInParentPathPerFolder: List[Int] =
      folderRowsSortedByParentPath.map { folderRow =>
        val parentPathSplitBySlash: Array[String] = folderRow.parentPath.getOrElse("").split("/")
        if (parentPathSplitBySlash.head.isEmpty || parentPathSplitBySlash.isEmpty) 0 else parentPathSplitBySlash.length
      }

    val slashesInParentPathsIncreaseByOne: Boolean =
      numberOfSlashesInParentPathPerFolder.zip(numberOfSlashesInParentPathPerFolder.drop(1)).forall {
        case (slashesInParentOfParent, slashesInParent) => (slashesInParent - slashesInParentOfParent) == 1
      }

    if (!slashesInParentPathsIncreaseByOne)
      IO.raiseError {
        new Exception(
          s"The lengths of the parent paths should increase by 1 for each subfolder (from 0 to N); " +
            s"instead it was ${numberOfSlashesInParentPathPerFolder.mkString(", ")}"
        )
      }
    else IO(numberOfSlashesInParentPathPerFolder)
  }

  private def checkEachParentPathMatchesFolderBeforeIt(
      folderRowsSortedByParentPath: List[DynamoTable]
  ): IO[Seq[Unit]] = {
    val folderRowsSortedByLongestParentPath: List[DynamoTable] = folderRowsSortedByParentPath.reverse

    val subfoldersWithPresumedParents: Seq[(DynamoTable, DynamoTable)] =
      folderRowsSortedByLongestParentPath.zip(folderRowsSortedByLongestParentPath.drop(1))

    subfoldersWithPresumedParents.map { case (subfolderInfo, presumedParentFolderInfo) =>
      val directParentRefOfSubfolder: String = subfolderInfo.parentPath.getOrElse("").split("/").last

      if (directParentRefOfSubfolder != presumedParentFolderInfo.id.toString) {
        IO.raiseError {
          new Exception(
            s"The parent ref of subfolder ${subfolderInfo.id} is $directParentRefOfSubfolder: " +
              s"this does not match the id of its presumed parent ${presumedParentFolderInfo.id}"
          )
        }
      } else IO.unit
    }.sequence
  }

  private def getEntitiesByIdentifier(
      folderRowsSortedByParentPath: List[DynamoTable],
      entitiesClient: EntityClient[IO, Fs2Streams[IO]]
  ): IO[List[Seq[Entity]]] =
    folderRowsSortedByParentPath.map { folderRow =>
      entitiesClient.entitiesByIdentifier(Identifier(sourceId, folderRow.name))
    }.sequence

  private def verifyOnlyOneEntityReturnedAndGetFullFolderInfo(
      potentialEntitiesWithSourceId: List[(DynamoTable, Seq[Entity])]
  ): IO[Map[UUID, FullFolderInfo]] =
    IO {
      potentialEntitiesWithSourceId.map { case (folderRow, potentialEntitiesWithSourceId) =>
        if (potentialEntitiesWithSourceId.length > 1) {
          throw new Exception(s"There is more than 1 entity with the same SourceID as ${folderRow.id}")
        } else {
          val potentialEntity = potentialEntitiesWithSourceId.headOption

          folderRow.id -> FullFolderInfo(folderRow, potentialEntity)
        }
      }.toMap
    }

  private def getExpectedParentRefForEachFolder(
      folderIdAndInfo: Map[UUID, FullFolderInfo]
  ): IO[List[FullFolderInfo]] =
    IO {
      folderIdAndInfo.map { case (_, fullFolderInfo) =>
        val directParent = fullFolderInfo.folderRow.parentPath
          .flatMap(_.split("/").lastOption)
          .map(UUID.fromString)
        directParent.flatMap(folderIdAndInfo.get) match {
          case None => fullFolderInfo // top-level folder doesn't/shouldn't have parent path
          case Some(fullFolderInfoOfParent) =>
            fullFolderInfoOfParent.entity
              .map(entity => fullFolderInfo.copy(expectedParentRef = Option(entity.ref)))
              .getOrElse(fullFolderInfo)
        }
      }.toList
    }

  private def createFolders(
      folderInfoOfEntities: List[FullFolderInfo],
      entitiesClient: EntityClient[IO, Fs2Streams[IO]],
      secretName: String,
      previouslyCreatedEntityIdsWithFolderRowIdsAsKeys: Map[UUID, UUID] = Map()
  ): IO[Unit] = {
    if (folderInfoOfEntities.isEmpty) IO.unit
    else {
      val folderInfo = folderInfoOfEntities.head
      val potentialParentRef =
        if (folderInfo.expectedParentRef.isEmpty) {
          // 'expectedParentRef' is empty either because parent was not in Preservica at start of Lambda, or folder is top-level
          val parentId = folderInfo.folderRow.parentPath
            .flatMap(_.split("/").lastOption)
            .map(UUID.fromString)
          parentId.flatMap(previouslyCreatedEntityIdsWithFolderRowIdsAsKeys.get)
        } else folderInfo.expectedParentRef

      val addFolderRequest = AddEntityRequest(
        None,
        folderInfo.folderRow.title.getOrElse(folderInfo.folderRow.name),
        folderInfo.folderRow.description,
        structuralObject,
        Open,
        potentialParentRef
      )

      val folderName = folderInfo.folderRow.name
      val identifiersToAdd = List(Identifier(sourceId, folderName)) ++
        folderInfo.folderRow.identifiers.map(id => Identifier(id.identifierName, id.value))

      for {
        entityId <- entitiesClient.addEntity(addFolderRequest)
        _ <- identifiersToAdd.map { identifierToAdd =>
          entitiesClient.addIdentifierForEntity(
            entityId,
            structuralObject,
            identifierToAdd
          )
        }.sequence
        _ <- createFolders(
          folderInfoOfEntities.tail,
          entitiesClient,
          secretName,
          previouslyCreatedEntityIdsWithFolderRowIdsAsKeys + (folderInfo.folderRow.id -> entityId)
        )
      } yield ()
    }
  }

  private def verifyEntitiesAreStructuralObjects(folderInfoOfEntitiesThatExist: List[FullFolderInfo]): IO[List[Unit]] =
    folderInfoOfEntitiesThatExist.map { folderInfo =>
      val potentialEntityType: Option[EntityClient.EntityType] = folderInfo.entity.flatMap(_.entityType)

      potentialEntityType
        .collect {
          case entityType if entityType != StructuralObject =>
            IO.raiseError(
              new Exception(
                s"The entity type for folder id ${folderInfo.folderRow.id} should be 'StructuralObject' but it is $entityType"
              )
            )
          case _ => IO.unit
        }
        .getOrElse(IO.raiseError(new Exception(s"There is no entity type for folder id ${folderInfo.folderRow.id}")))
    }.sequence

  private def verifyExpectedParentFolderMatchesFolderFromApiAndGetSecurityTag(
      folderInfoOfEntitiesThatExist: List[FullFolderInfo]
  ): IO[List[FullFolderInfo]] =
    folderInfoOfEntitiesThatExist.map { folderInfo =>
      val entity = folderInfo.entity.get
      val ref = entity.ref
      val isNotTopLevelFolder = folderInfo.folderRow.parentPath.isDefined
      val parentRef = entity.parent

      /* Top-level folder's parentRef will be different from its expectedParentRef (of "") as it's not possible to know
      parentRef before calling API but since its a top-level folder, we don't have to worry about it not having the correct parent */
      if (parentRef != folderInfo.expectedParentRef && isNotTopLevelFolder)
        IO.raiseError {
          new Exception(
            s"API returned a parent ref of '${parentRef.getOrElse("None")}' for entity $ref instead of expected '${folderInfo.expectedParentRef.getOrElse("")}'"
          )
        }
      else
        entity.securityTag match {
          case open @ Some(Open)     => IO(folderInfo.copy(securityTag = open))
          case closed @ Some(Closed) => IO(folderInfo.copy(securityTag = closed))
          case unexpectedTag =>
            IO.raiseError(new Exception(s"Security tag '$unexpectedTag' is unexpected for SO ref '$ref'"))
        }
    }.sequence

  private def findIdentifiersToUpdate(
      identifiersFromDynamo: List[Identifier],
      identifiersFromPreservica: Seq[IdentifierResponse]
  ): IO[Seq[IdentifierToUpdate]] = IO {
    identifiersFromDynamo.flatMap { id =>
      identifiersFromPreservica
        .find(pid => pid.identifierName == id.identifierName && pid.value != id.value)
        .map(pid => IdentifierToUpdate(pid, IdentifierResponse(pid.id, id.identifierName, id.value)))
    }
  }

  private def findIdentifiersToAdd(
      identifiersFromDynamo: Seq[Identifier],
      identifiersFromPreservica: Seq[IdentifierResponse]
  ): IO[Seq[Identifier]] = IO {
    identifiersFromDynamo
      .filterNot { id =>
        identifiersFromPreservica.exists(pid => pid.identifierName == id.identifierName)
      }
      .map(id => Identifier(id.identifierName, id.value))
  }

  private def findOnlyFoldersThatNeedUpdatingAndCreateRequests(
      folderInfoOfEntitiesThatExist: List[FullFolderInfo]
  ): List[EntityWithUpdateEntityRequest] =
    folderInfoOfEntitiesThatExist.flatMap { folderInfo =>
      val folderRow = folderInfo.folderRow
      val entity = folderInfo.entity.get

      val potentialNewTitle = folderRow.title.orElse(entity.title)
      val potentialNewDescription = folderRow.description

      val titleHasChanged = potentialNewTitle != entity.title && potentialNewTitle.nonEmpty
      val descriptionHasChanged = potentialNewDescription != entity.description && potentialNewDescription.nonEmpty

      val updateEntityRequest =
        if (titleHasChanged || descriptionHasChanged) {
          val parentRef = folderInfo.expectedParentRef
          val updatedTitleOrDescriptionRequest =
            UpdateEntityRequest(
              entity.ref,
              potentialNewTitle.get,
              if (descriptionHasChanged) potentialNewDescription else None,
              structuralObject,
              folderInfo.securityTag.get,
              parentRef
            )
          Some(updatedTitleOrDescriptionRequest)
        } else None

      updateEntityRequest.map(EntityWithUpdateEntityRequest(entity, _))
    }
}

object Lambda extends App {
  private case class Config(apiUrl: String, secretName: String, archiveFolderTableName: String)

  private case class StepFnInput(
      batchId: String,
      archiveHierarchyFolders: List[String],
      contentFolders: List[String],
      contentAssets: List[String]
  )

  private case class FullFolderInfo(
      folderRow: DynamoTable,
      entity: Option[Entity],
      expectedParentRef: Option[UUID] = None,
      securityTag: Option[SecurityTag] = None
  )
  private[nationalarchives] case class EntityWithUpdateEntityRequest(
      entity: Entity,
      updateEntityRequest: UpdateEntityRequest
  )

  private[nationalarchives] case class IdentifierToUpdate(
      oldIdentifier: IdentifierResponse,
      newIdentifier: IdentifierResponse
  )
}

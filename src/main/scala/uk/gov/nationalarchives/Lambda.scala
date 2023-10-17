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
import org.scanamo.generic.auto._
import io.circe.generic.auto._
import org.scanamo.{DynamoFormat, DynamoReadError, DynamoValue, MissingProperty}
import uk.gov.nationalarchives.Lambda.{
  Config,
  EntityWithUpdateEntityRequest,
  FullFolderInfo,
  GetItemsResponse,
  PartitionKey,
  StepFnInput,
  UpdatedIdentifier
}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, Identifier, IdentifierResponse}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.{
  AddEntityRequest,
  Closed,
  Open,
  SecurityTag,
  StructuralObject,
  UpdateEntityRequest
}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import upickle.default

import java.io.{InputStream, OutputStream}
import java.util.UUID
import scala.io.Source
import scala.jdk.CollectionConverters._

class Lambda extends RequestStreamHandler {
  lazy val eventBridgeClient: DAEventBridgeClient[IO] = DAEventBridgeClient[IO]()

  implicit val format: DynamoFormat[GetItemsResponse] = new DynamoFormat[GetItemsResponse] {
    override def read(av: DynamoValue): Either[DynamoReadError, GetItemsResponse] = {
      val map = av.toAttributeValue.m().asScala
      def value(name: String) = map.get(name).map(_.s())
      def valueOrLeft(name: String) = value(name).toRight(MissingProperty)
      for {
        id <- valueOrLeft("id")
        name <- valueOrLeft("name")
      } yield {
        val identifiers = map
          .filter(_._1.startsWith("id_"))
          .map { case (name, value) =>
            Identifier(name.drop(3), value.s())
          }
          .toList
        GetItemsResponse(id, value("parentPath").getOrElse(""), name, value("title"), value("description"), identifiers)
      }
    }

    override def write(t: GetItemsResponse): DynamoValue = DynamoValue.nil
  }

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
      stepFnInput.archiveHierarchyFolders.map(PartitionKey)

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
          identifiersFromPreservica <- entitiesClient.getIdentifiersForEntity(entity)
          updatedIdentifier <- findIdentifiersToUpdate(identifiersFromDynamo, identifiersFromPreservica)
          identifiersToAdd <- findIdentifiersToAdd(identifiersFromDynamo, identifiersFromPreservica)
          _ <- identifiersToAdd.map { id =>
            entitiesClient.addIdentifierForEntity(entity.ref, entity.entityType.getOrElse(StructuralObject), id)
          }.sequence
          newIdentifier = updatedIdentifier.map(_.newIdentifier)
          _ <-
            if (newIdentifier.nonEmpty) entitiesClient.updateIdentifiers(entity, updatedIdentifier.map(_.newIdentifier))
            else IO.unit
          updatedSlackMessage <- generateIdentifierSlackMessage(
            config.apiUrl,
            entity,
            updatedIdentifier,
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
      updatedIdentifier: Seq[UpdatedIdentifier],
      addedIdentifiers: Seq[Identifier]
  ): IO[Option[String]] = IO {
    if (updatedIdentifier.isEmpty && addedIdentifiers.isEmpty) {
      None
    } else
      Option {
        val firstLine = generateSlackMessageFirstLine(preservicaUrl, entity)
        firstLine + updatedIdentifier.headOption
          .map(_ => "The following identifiers have been updated\n")
          .getOrElse("") +
          updatedIdentifier
            .map { ui =>
              s"""
           |*Old value* ${ui.oldIdentifier.identifierName}: ${ui.oldIdentifier.value}
           |*New value* ${ui.newIdentifier.identifierName}: ${ui.newIdentifier.value}
           |
           |""".stripMargin
            }
            .mkString("") +
          addedIdentifiers.headOption.map(_ => "The following identifiers have been added\n").getOrElse("") +
          addedIdentifiers
            .map { ai =>
              s"""
           |${ai.identifierName}: ${ai.value}
           |""".stripMargin
            }
            .mkString("")
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
  ): IO[List[GetItemsResponse]] = {
    val getItemsResponse: IO[List[GetItemsResponse]] =
      dADynamoDBClient.getItems[GetItemsResponse, PartitionKey](
        folderIdPartitionKeysAndValues,
        archiveFolderTableName
      )

    getItemsResponse.map(_.sortBy(folderRow => folderRow.parentPath))
  }

  private def checkNumOfParentPathSlashesPerFolderIncrease(
      folderRowsSortedByParentPath: List[GetItemsResponse]
  ): IO[List[Int]] = {
    val numberOfSlashesInParentPathPerFolder: List[Int] =
      folderRowsSortedByParentPath.map { folderRow =>
        val parentPathSplitBySlash: Array[String] = folderRow.parentPath.split("/")
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
      folderRowsSortedByParentPath: List[GetItemsResponse]
  ): IO[Seq[Unit]] = {
    val folderRowsSortedByLongestParentPath: List[GetItemsResponse] = folderRowsSortedByParentPath.reverse

    val subfoldersWithPresumedParents: Seq[(GetItemsResponse, GetItemsResponse)] =
      folderRowsSortedByLongestParentPath.zip(folderRowsSortedByLongestParentPath.drop(1))

    subfoldersWithPresumedParents.map { case (subfolderInfo, presumedParentFolderInfo) =>
      val directParentRefOfSubfolder: String = subfolderInfo.parentPath.split("/").last

      if (directParentRefOfSubfolder != presumedParentFolderInfo.id) {
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
      folderRowsSortedByParentPath: List[GetItemsResponse],
      entitiesClient: EntityClient[IO, Fs2Streams[IO]]
  ): IO[List[Seq[Entity]]] =
    folderRowsSortedByParentPath.map { folderRow =>
      entitiesClient.entitiesByIdentifier(Identifier(sourceId, folderRow.name))
    }.sequence

  private def verifyOnlyOneEntityReturnedAndGetFullFolderInfo(
      potentialEntitiesWithSourceId: List[(GetItemsResponse, Seq[Entity])]
  ): IO[Map[String, FullFolderInfo]] =
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
      folderIdAndInfo: Map[String, FullFolderInfo]
  ): IO[List[FullFolderInfo]] =
    IO {
      folderIdAndInfo.map { case (_, fullFolderInfo) =>
        val directParent = fullFolderInfo.folderRow.parentPath.split("/").last
        folderIdAndInfo.get(directParent) match {
          case None => fullFolderInfo // top-level folder doesn't/shouldn't have parent path
          case Some(fullFolderInfoOfParent) =>
            fullFolderInfoOfParent.entity
              .map(entity => fullFolderInfo.copy(expectedParentRef = entity.ref.toString))
              .getOrElse(fullFolderInfo)
        }
      }.toList
    }

  private def createFolders(
      folderInfoOfEntities: List[FullFolderInfo],
      entitiesClient: EntityClient[IO, Fs2Streams[IO]],
      secretName: String,
      previouslyCreatedEntityIdsWithFolderRowIdsAsKeys: Map[String, UUID] = Map()
  ): IO[Unit] = {
    if (folderInfoOfEntities.isEmpty) IO.unit
    else {
      val folderInfo = folderInfoOfEntities.head
      val potentialParentRef =
        if (folderInfo.expectedParentRef.isEmpty) {
          // 'expectedParentRef' is empty either because parent was not in Preservica at start of Lambda, or folder is top-level
          val parentId = folderInfo.folderRow.parentPath.split("/").last
          previouslyCreatedEntityIdsWithFolderRowIdsAsKeys.get(parentId)
        } else Some(UUID.fromString(folderInfo.expectedParentRef))

      val addFolderRequest = AddEntityRequest(
        None,
        folderInfo.folderRow.title.getOrElse(folderInfo.folderRow.name),
        folderInfo.folderRow.description,
        structuralObject,
        Open,
        potentialParentRef
      )

      val folderName = folderInfo.folderRow.name
      val identifiersToAdd = List(Identifier(sourceId, folderName)) ++ folderInfo.folderRow.identifiers

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
      val isNotTopLevelFolder = folderInfo.folderRow.parentPath != ""
      val parentRef = entity.parent.map(_.toString).getOrElse("")

      /* Top-level folder's parentRef will be different from its expectedParentRef (of "") as it's not possible to know
      parentRef before calling API but since its a top-level folder, we don't have to worry about it not having the correct parent */
      if (parentRef != folderInfo.expectedParentRef && isNotTopLevelFolder)
        IO.raiseError {
          new Exception(
            s"API returned a parent ref of '$parentRef' for entity $ref instead of expected ${folderInfo.expectedParentRef}"
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
      identifiersFromDynamo: Seq[Identifier],
      identifiersFromPreservica: Seq[IdentifierResponse]
  ): IO[Seq[UpdatedIdentifier]] = IO {
    identifiersFromDynamo.flatMap { id =>
      identifiersFromPreservica
        .find(pid => pid.identifierName == id.identifierName && pid.value != id.value)
        .map(pid => UpdatedIdentifier(pid, IdentifierResponse(pid.id, id.identifierName, id.value)))
    }
  }

  private def findIdentifiersToAdd(
      identifiersFromDynamo: Seq[Identifier],
      identifiersFromPreservica: Seq[IdentifierResponse]
  ): IO[Seq[Identifier]] = IO {
    identifiersFromDynamo.filter { id =>
      !identifiersFromPreservica.exists(pid => pid.identifierName == id.identifierName)
    }
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
          val parentRef = Option.when(folderInfo.expectedParentRef != "")(UUID.fromString(folderInfo.expectedParentRef))
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
  case class PartitionKey(id: String) {
    UUID.fromString(id)
  }
  case class GetItemsResponse(
      id: String,
      parentPath: String,
      name: String,
      title: Option[String],
      description: Option[String],
      identifiers: Seq[Identifier] = Nil
  )
  private case class Config(apiUrl: String, secretName: String, archiveFolderTableName: String)

  private case class StepFnInput(
      batchId: String,
      archiveHierarchyFolders: List[String],
      contentFolders: List[String],
      contentAssets: List[String]
  )

  private case class FullFolderInfo(
      folderRow: GetItemsResponse,
      entity: Option[Entity],
      expectedParentRef: String = "",
      securityTag: Option[SecurityTag] = None
  )
  private[nationalarchives] case class EntityWithUpdateEntityRequest(
      entity: Entity,
      updateEntityRequest: UpdateEntityRequest
  )

  private[nationalarchives] case class UpdatedIdentifier(
      oldIdentifier: IdentifierResponse,
      newIdentifier: IdentifierResponse
  )
}

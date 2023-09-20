package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import org.scanamo.generic.auto.genericDerivedFormat
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import software.amazon.awssdk.services.eventbridge.model.PutEventsResponse
import sttp.capabilities.fs2.Fs2Streams
import io.circe.generic.auto._
import uk.gov.nationalarchives.Lambda.{
  Config,
  EntityWithUpdateEntityRequest,
  FullFolderInfo,
  GetItemsResponse,
  PartitionKey,
  StepFnInput
}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, Identifier}
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

class Lambda extends RequestStreamHandler {
  lazy val eventBridgeClient: DAEventBridgeClient[IO] = DAEventBridgeClient[IO]()

  lazy val entitiesClientIO: IO[EntityClient[IO, Fs2Streams[IO]]] = configIo.flatMap { config =>
    Fs2Client.entityClient(config.apiUrl)
  }
  val dADynamoDBClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  private val parentRefNodeName = "Parent"
  private val structuralObject = StructuralObject
  private val securityTagName = "SecurityTag"
  private val sourceId = "SourceId"

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

      potentialEntitiesWithSourceId <- getEntitiesByIdentifier(folderRowsSortedByParentPath, entitiesClient, secretName)
      folderIdAndInfo <-
        verifyOnlyOneEntityReturnedAndGetFullFolderInfo(folderRowsSortedByParentPath.zip(potentialEntitiesWithSourceId))
      folderInfoWithExpectedParentRef <- getExpectedParentRefForEachFolder(folderIdAndInfo)

      (folderInfoOfEntitiesThatDoNotExist, folderInfoOfEntitiesThatExist) = folderInfoWithExpectedParentRef.partition(
        _.entity.isEmpty
      )
      _ <- createFolders(folderInfoOfEntitiesThatDoNotExist, entitiesClient, secretName)
      _ <- verifyEntitiesAreStructuralObjects(folderInfoOfEntitiesThatExist)

      folderInfoOfEntitiesThatExistWithSecurityTags <-
        verifyExpectedParentFolderMatchesFolderFromApiAndGetSecurityTag(
          folderInfoOfEntitiesThatExist,
          entitiesClient,
          secretName
        )
      folderUpdateRequests = findOnlyFoldersThatNeedUpdatingAndCreateRequests(
        folderInfoOfEntitiesThatExistWithSecurityTags
      )
      _ <- folderUpdateRequests.map { folderUpdateRequest =>
        val message = generateSlackMessage(folderUpdateRequest)
        for {
          _ <- entitiesClient.updateEntity(folderUpdateRequest.updateEntityRequest, secretName)
          _ <- sendToSlack(message)
        } yield ()
      }.sequence
    } yield ()
  }.unsafeRunSync()

  private def generateSlackMessage(folderUpdateRequest: EntityWithUpdateEntityRequest): String = {
    val entity = folderUpdateRequest.entity
    val updateEntityRequest = folderUpdateRequest.updateEntityRequest
    s""":preservica: Entity ${entity.ref} has been updated
         |*Old title*: ${entity.title.getOrElse("")}
         |*New title*: ${updateEntityRequest.titleToChange}
         |*Old description*: ${entity.description.getOrElse("")}
         |*New description*: ${updateEntityRequest.descriptionToChange.getOrElse("")}
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
      entitiesClient: EntityClient[IO, Fs2Streams[IO]],
      secretName: String
  ): IO[List[Seq[Entity]]] =
    folderRowsSortedByParentPath.map { folderRow =>
      entitiesClient.entitiesByIdentifier(Identifier(sourceId, folderRow.name), secretName)
    }.sequence

  private def verifyOnlyOneEntityReturnedAndGetFullFolderInfo(
      potentialEntitiesWithSourceId: List[(GetItemsResponse, Seq[Entity])]
  ): IO[Map[String, FullFolderInfo]] =
    IO {
      potentialEntitiesWithSourceId.map { case (folderRow, potentialEntitiesWithSourceId) =>
        if (potentialEntitiesWithSourceId.length > 1) {
          throw new Exception(s"There is more than 1 entity with the same SourceId as ${folderRow.id}")
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
        folderInfo.folderRow.title,
        folderInfo.folderRow.description,
        structuralObject,
        Open,
        potentialParentRef
      )

      val folderName = folderInfo.folderRow.name
      val identifiersToAdd = List(Identifier(sourceId, folderName), Identifier("Code", folderName))

      for {
        entityId <- entitiesClient.addEntity(addFolderRequest, secretName)
        _ <- identifiersToAdd.map { identifierToAdd =>
          entitiesClient.addIdentifierForEntity(
            entityId,
            structuralObject,
            identifierToAdd,
            secretName
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
      val entityType: String = folderInfo.entity.get.entityType.getOrElse("")
      if (entityType != "SO")
        IO.raiseError(
          new Exception(s"The entity type for ${folderInfo.folderRow.id} should be SO but it is $entityType")
        )
      else IO.unit
    }.sequence

  private def verifyExpectedParentFolderMatchesFolderFromApiAndGetSecurityTag(
      folderInfoOfEntitiesThatExist: List[FullFolderInfo],
      entitiesClient: EntityClient[IO, Fs2Streams[IO]],
      secretName: String
  ): IO[List[FullFolderInfo]] =
    folderInfoOfEntitiesThatExist.map { folderInfo =>
      val entity = folderInfo.entity.get
      val ref = entity.ref
      val isTopLevelFolder = folderInfo.folderRow.parentPath == ""
      val childNodeNames = (if (isTopLevelFolder) Nil else List(parentRefNodeName)) ++ List(securityTagName)

      entitiesClient
        .nodesFromEntity(ref, structuralObject, childNodeNames, secretName)
        .flatMap { nodeNamesAndValues =>
          val parentRef = nodeNamesAndValues.getOrElse(parentRefNodeName, "")

          /* Top-level folder's parentRef will be different from its expectedParentRef (of "") as it's not possible to know
          parentRef before calling API but since its a top-level folder, we don't have to worry about it not having the correct parent */
          if (parentRef != folderInfo.expectedParentRef && !isTopLevelFolder)
            IO.raiseError {
              new Exception(
                s"API returned a parent ref of '$parentRef' for entity $ref instead of expected ${folderInfo.expectedParentRef}"
              )
            }
          else {
            val securityTag: String = nodeNamesAndValues.getOrElse(securityTagName, "")
            securityTag match {
              case "open"   => IO(folderInfo.copy(securityTag = Some(Open)))
              case "closed" => IO(folderInfo.copy(securityTag = Some(Closed)))
              case unexpectedTag =>
                IO.raiseError(new Exception(s"Security tag '$unexpectedTag' is unexpected for SO ref '$ref'"))
            }
          }
        }
    }.sequence

  private def findOnlyFoldersThatNeedUpdatingAndCreateRequests(
      folderInfoOfEntitiesThatExist: List[FullFolderInfo]
  ): List[EntityWithUpdateEntityRequest] =
    folderInfoOfEntitiesThatExist.flatMap { folderInfo =>
      val folderRow = folderInfo.folderRow
      val entity = folderInfo.entity.get

      val potentialNewTitle = folderRow.title.getOrElse("")
      val potentialNewDescription = folderRow.description

      val titleHasChanged = potentialNewTitle != entity.title.getOrElse("")
      val descriptionHasChanged = potentialNewDescription.getOrElse("") != entity.description.getOrElse("")

      val updateEntityRequest =
        if (titleHasChanged || descriptionHasChanged) {
          val parentRef = Option.when(folderInfo.expectedParentRef != "")(UUID.fromString(folderInfo.expectedParentRef))
          val updatedTitleOrDescriptionRequest =
            UpdateEntityRequest(
              entity.ref,
              potentialNewTitle,
              potentialNewDescription,
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
      description: Option[String]
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
}

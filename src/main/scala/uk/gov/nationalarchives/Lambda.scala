package uk.gov.nationalarchives

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits._
import com.amazonaws.services.lambda.runtime.{Context, RequestStreamHandler}
import org.scanamo.generic.auto.genericDerivedFormat
import pureconfig._
import pureconfig.generic.auto._
import pureconfig.module.catseffect.syntax._
import sttp.capabilities.fs2.Fs2Streams
import uk.gov.nationalarchives.Lambda.{Config, FullFolderInfo, GetItemsResponse, PartitionKey, StepFnInput}
import uk.gov.nationalarchives.dp.client.Entities.{Entity, Identifier}
import uk.gov.nationalarchives.dp.client.EntityClient
import uk.gov.nationalarchives.dp.client.EntityClient.{AddEntityRequest, Closed, Open, SecurityTag, UpdateEntityRequest}
import uk.gov.nationalarchives.dp.client.fs2.Fs2Client
import upickle.default

import java.io.{InputStream, OutputStream}
import java.util.UUID
import scala.io.Source

class Lambda extends RequestStreamHandler {
  lazy val entitiesClientIO: IO[EntityClient[IO, Fs2Streams[IO]]] = configIo.flatMap { config =>
    Fs2Client.entityClient(config.apiUrl)
  }
  val dADynamoDBClient: DADynamoDBClient[IO] = DADynamoDBClient[IO]()
  private val parentRefNodeName = "ParentRef"
  private val structuralObject = "structural-objects"
  private val securityTagName = "SecurityTag"
  private val sourceId = "SourceId"

  private val configIo: IO[Config] = ConfigSource.default.loadF[IO, Config]()
  private implicit val secretRW: default.ReadWriter[StepFnInput] = default.macroRW[StepFnInput]

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

      folderIdAndInfo <- {
        val folderIdMappedToFolderInfo: List[IO[(String, FullFolderInfo)]] =
          folderRowsSortedByParentPath.map { folderRow =>
            val potentialEntitiesWithSourceIdIo: IO[Seq[Entity]] =
              entitiesClient.entitiesByIdentifier(Identifier(sourceId, folderRow.name), secretName)
            verifyOnlyOneEntityReturnedAndGetFullFolderInfo(potentialEntitiesWithSourceIdIo, folderRow)
          }
        folderIdMappedToFolderInfo.sequence.map(_.toMap)
      }

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
      _ <- folderUpdateRequests
        .map(folderUpdateRequest => entitiesClient.updateEntity(folderUpdateRequest, secretName))
        .sequence
    } yield ()
  }.unsafeRunSync()

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

    if (numberOfSlashesInParentPathPerFolder != List(0, 1, 2))
      IO.raiseError {
        new Exception(
          "The lengths of the parent paths should increase for each subfolder (from 0 to 2); this is not the case"
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

  private def verifyOnlyOneEntityReturnedAndGetFullFolderInfo(
      potentialEntitiesWithSourceId: IO[Seq[Entity]],
      folderRow: GetItemsResponse
  ): IO[(String, FullFolderInfo)] =
    potentialEntitiesWithSourceId.map { potentialEntitiesWithSourceId =>
      if (potentialEntitiesWithSourceId.length > 1) {
        throw new Exception(s"There is more than 1 entity with the same SourceId as ${folderRow.id}")
      } else {
        val potentialEntity = potentialEntitiesWithSourceId.headOption

        folderRow.id -> FullFolderInfo(folderRow, potentialEntity)
      }
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
      foldersPreviouslyAdded: Map[String, UUID] = Map()
  ): IO[Unit] = {
    if (folderInfoOfEntities.isEmpty) IO.unit
    else {
      val folderInfo = folderInfoOfEntities.head

      val folderName = folderInfo.folderRow.name
      val identifiersToAdd = List(Identifier(sourceId, folderName), Identifier("Code", folderName))

      val parentRef = if (folderInfo.expectedParentRef.isEmpty) {
        val parentId = folderInfo.folderRow.parentPath.split("/").last
        foldersPreviouslyAdded(parentId).toString
      } else folderInfo.expectedParentRef

      val addFolderRequest = AddEntityRequest(
        None,
        folderInfo.folderRow.title,
        folderInfo.folderRow.description,
        structuralObject,
        Open,
        Some(UUID.fromString(parentRef))
      )
      for {
        entityId <- entitiesClient.addEntity(addFolderRequest, secretName)
        _ <- entitiesClient.addIdentifiersForEntity(
          entityId,
          structuralObject,
          identifiersToAdd,
          secretName
        )
        _ <- createFolders(
          folderInfoOfEntities.tail,
          entitiesClient,
          secretName,
          foldersPreviouslyAdded + (folderInfo.folderRow.id -> entityId)
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

      entitiesClient
        .nodesFromEntity(ref, structuralObject, List(parentRefNodeName, securityTagName), secretName)
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
  ): List[UpdateEntityRequest] =
    folderInfoOfEntitiesThatExist.flatMap { folderInfo =>
      val folderRow = folderInfo.folderRow
      val entity = folderInfo.entity.get

      val updateEntityRequestWithNoUpdates =
        UpdateEntityRequest(entity.ref, None, None, structuralObject, folderInfo.securityTag.get, None)

      val potentiallyUpdatedTitleRequest =
        if (folderRow.title.getOrElse("") != entity.title.getOrElse(""))
          updateEntityRequestWithNoUpdates.copy(titleToChange = folderRow.title)
        else updateEntityRequestWithNoUpdates

      val potentiallyUpdatedTitleOrDescriptionRequest =
        if (folderRow.description.getOrElse("") != entity.description.getOrElse(""))
          potentiallyUpdatedTitleRequest.copy(descriptionToChange = folderRow.description)
        else potentiallyUpdatedTitleRequest

      if (potentiallyUpdatedTitleOrDescriptionRequest != updateEntityRequestWithNoUpdates)
        Some(potentiallyUpdatedTitleOrDescriptionRequest)
      else None
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
      rootPath: String,
      batchType: String,
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
}

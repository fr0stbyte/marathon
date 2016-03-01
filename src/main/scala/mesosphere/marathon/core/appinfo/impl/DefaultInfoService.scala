package mesosphere.marathon.core.appinfo.impl

import mesosphere.marathon.core.appinfo.AppInfo
import mesosphere.marathon.core.appinfo._
import mesosphere.marathon.state._
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.collection.immutable.Seq

private[appinfo] class DefaultInfoService(
    groupManager: GroupManager,
    appRepository: AppRepository,
    newBaseData: () => AppInfoBaseData) extends AppInfoService with GroupInfoService {
  import scala.concurrent.ExecutionContext.Implicits.global

  private[this] val log = LoggerFactory.getLogger(getClass)

  override def queryForAppId(id: PathId, embed: Set[AppInfo.Embed]): Future[Option[AppInfo]] = {
    log.debug(s"queryForAppId $id")
    appRepository.currentVersion(id).flatMap {
      case Some(app) => newBaseData().appInfoFuture(app, embed).map(Some(_))
      case None      => Future.successful(None)
    }
  }

  override def queryAll(selector: AppSelector, embed: Set[AppInfo.Embed]): Future[Seq[AppInfo]] = {
    log.debug(s"queryAll")
    groupManager.rootGroup()
      .map(_.transitiveApps.filter(selector.matches))
      .flatMap(resolveAppInfos(_, embed))
  }

  override def queryAllInGroup(groupId: PathId, embed: Set[AppInfo.Embed]): Future[Seq[AppInfo]] = {
    log.debug(s"queryAllInGroup $groupId")
    groupManager
      .group(groupId)
      .map(_.map(_.transitiveApps).getOrElse(Seq.empty))
      .flatMap(resolveAppInfos(_, embed))
  }

  override def queryForGroup(group: Group,
                             appEmbed: Set[AppInfo.Embed],
                             groupEmbed: Set[GroupInfo.Embed]): Future[GroupInfo] = {

    def queryGroups =
      if (groupEmbed(GroupInfo.Embed.Groups))
        group.groups.foldLeft(Future.successful(List.empty[GroupInfo])) {
          case (future, subGroup) =>
            future.flatMap(result => queryForGroup(subGroup, appEmbed, groupEmbed).map(info => info :: result))
        }.map(list => Some(list.sortBy(_.group.id)))
      else Future.successful(None)

    def queryApps =
      if (groupEmbed(GroupInfo.Embed.Apps))
        resolveAppInfos(group.apps, appEmbed).map(list => Some(list.sortBy(_.app.id)))
      else Future.successful(None)

    for {
      groups <- queryGroups
      apps <- queryApps
    } yield GroupInfo(group, apps, groups)
  }

  private[this] def resolveAppInfos(apps: Iterable[AppDefinition], embed: Set[AppInfo.Embed]): Future[Seq[AppInfo]] = {
    val baseData = newBaseData()

    apps
      .foldLeft(Future.successful(Seq.newBuilder[AppInfo])) {
        case (builderFuture, app) =>
          builderFuture.flatMap { builder =>
            baseData.appInfoFuture(app, embed).map { appInfo =>
              builder += appInfo
              builder
            }
          }
      }.map(_.result())
  }
}

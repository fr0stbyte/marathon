package mesosphere.marathon.core.appinfo

import mesosphere.marathon.state.Group

import scala.concurrent.Future

/**
  * Queries for extended group information.
  */
trait GroupInfoService {

  /**
    * Query info for an existing group.
    */
  def queryForGroup(group: Group, appEmbed: Set[AppInfo.Embed], groupEmbed: Set[GroupInfo.Embed]): Future[GroupInfo]

}

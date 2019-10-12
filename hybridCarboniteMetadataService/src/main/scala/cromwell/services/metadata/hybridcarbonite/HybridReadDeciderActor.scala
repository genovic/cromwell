package cromwell.services.metadata.hybridcarbonite

import akka.actor.{ActorRef, FSM, LoggingFSM, Props}
import cromwell.services.metadata.MetadataService.{GetLabels, GetStatus, MetadataReadAction, MetadataServiceResponse, QueryForWorkflowsMatchingParameters, WorkflowMetadataReadAction, WorkflowQueryFailure, WorkflowQuerySuccess}
import cromwell.services.metadata.hybridcarbonite.HybridReadDeciderActor._
import cromwell.services.metadata.impl.builder.MetadataBuilderActor.FailedMetadataResponse
import cromwell.services.metadata.{MetadataArchiveStatus, WorkflowQueryKey}

import scala.concurrent.ExecutionContext

class HybridReadDeciderActor(classicMetadataServiceActor: ActorRef, carboniteMetadataServiceActor: ActorRef) extends LoggingFSM[HybridReadDeciderState, HybridReadDeciderData] {

  startWith(Pending, NoData)

  implicit val ec: ExecutionContext = context.dispatcher

  when(Pending) {
    case Event(labelsOrStatus @ (_: GetLabels | _: GetStatus), NoData) =>
      // The classicMetadataServiceActor is always the SOA for labels or status data.
      classicMetadataServiceActor forward labelsOrStatus
      stop(FSM.Normal)
    case Event(read: WorkflowMetadataReadAction, NoData) =>
      classicMetadataServiceActor ! QueryForWorkflowsMatchingParameters(Vector(WorkflowQueryKey.Id.name -> read.workflowId.toString))
      goto(RequestingMetadataArchiveStatus) using WorkingData(sender(), read)
    case Event(query: QueryForWorkflowsMatchingParameters, NoData) =>
      classicMetadataServiceActor ! query
      goto(WaitingForMetadataResponse) using WorkingData(sender(), query)
  }

  when(RequestingMetadataArchiveStatus) {
    case Event(WorkflowQuerySuccess(response, _), wd: WorkingData) if response.results.size > 1 =>
        val errorMsg = s"Programmer Error: Got more than one status row back looking up metadata archive status for ${wd.request}: $response"
        wd.actor ! makeAppropriateFailureForRequest(errorMsg, wd.request)
        stop(FSM.Failure(errorMsg))
    case Event(WorkflowQuerySuccess(response, _), wd: WorkingData) if response.results.headOption.exists(_.metadataArchiveStatus == MetadataArchiveStatus.Archived) =>
        carboniteMetadataServiceActor ! wd.request
        goto(WaitingForMetadataResponse)
    case Event(_: WorkflowQuerySuccess, wd: WorkingData) =>
        classicMetadataServiceActor ! wd.request
        goto(WaitingForMetadataResponse)
    case Event(WorkflowQueryFailure(reason), wd: WorkingData) =>
      log.error(reason, s"Programmer Error: Failed to determine how to route ${wd.request.getClass.getSimpleName}. Falling back to classic.")
      classicMetadataServiceActor ! wd.request
      goto(WaitingForMetadataResponse)
  }

  when(WaitingForMetadataResponse) {
    case Event(response: MetadataServiceResponse, wd: WorkingData) =>
      wd.actor ! response
      stop(FSM.Normal)
  }

  whenUnhandled {
    case Event(akkaMessage, data) =>
      val logMessage = s"Programmer Error: Unexpected message '$akkaMessage' sent to ${getClass.getSimpleName} in state $stateName with data $stateData from $sender()"
      log.error(logMessage)
      data match {
        case NoData =>
          stop(FSM.Failure(logMessage))
        case WorkingData(actor, request) =>
          actor ! makeAppropriateFailureForRequest(logMessage, request)
          stop(FSM.Failure(logMessage))
      }
  }

  def makeAppropriateFailureForRequest(msg: String, request: MetadataReadAction) = request match {
    case _: QueryForWorkflowsMatchingParameters => WorkflowQueryFailure(new Exception(msg))
    case _ => FailedMetadataResponse(request, new Exception(msg))
  }

}

object HybridReadDeciderActor {
  def props(classicMetadataServiceActor: ActorRef, carboniteMetadataServiceActor: ActorRef) =
    Props(new HybridReadDeciderActor(classicMetadataServiceActor, carboniteMetadataServiceActor))

  sealed trait HybridReadDeciderState
  case object Pending extends HybridReadDeciderState
  case object RequestingMetadataArchiveStatus extends HybridReadDeciderState
  case object WaitingForMetadataResponse extends HybridReadDeciderState

  sealed trait HybridReadDeciderData
  case object NoData extends HybridReadDeciderData
  final case class WorkingData(actor: ActorRef, request: MetadataReadAction) extends HybridReadDeciderData
}

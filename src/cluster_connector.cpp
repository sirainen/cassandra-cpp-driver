#include "cluster_connector.hpp"

#include "control_connector.hpp"
#include "event_loop.hpp"
#include "list.hpp"
#include "session_base.hpp"

namespace datastax { namespace internal { namespace core {

ClusterConnector::ClusterConnector(const AddressVec& contact_points,
                                   ProtocolVersion protocol_version,
                                   const ClusterSettings& settings, ClusterListener* listener,
                                   const Callback& callback)
    : contact_points_(contact_points)
    , protocol_version_(protocol_version)
    , settings_(settings)
    , listener_(listener)
    , callback_(callback)
    , event_loop_(NULL)
    , remaining_connector_count_(0)
    , error_code_(CLUSTER_OK) {}

void ClusterConnector::connect(EventLoop* event_loop) {
  event_loop_ = event_loop;
  inc_ref();
  resolver_.reset(new Resolver());
  resolver_->resolve(event_loop_, contact_points_, settings_.port,
                     bind_callback(&ClusterConnector::on_resolve, this));
}

void ClusterConnector::cancel() {
  if (resolver_) resolver_->cancel();
  for (ControlConnectorList::iterator it = pending_connectors_.begin(),
                                       end = pending_connectors_.end();
       it != end; ++it) {
    (*it)->cancel();
  }
  maybe_finish();
}

Cluster::Ptr ClusterConnector::release_cluster() {
  Cluster::Ptr temp(cluster_);
  cluster_.reset();
  return temp;
}

void ClusterConnector::on_resolve(Resolver* resolver) {
  if (resolver->is_canceled()) {
    finish();
    return;
  }

  AddressVec contact_points;
  if (resolver->is_ok()) {
    contact_points = resolver->addresses();
  } else {
    error_code_ = CLUSTER_ERROR_RESOLVE;
    error_message_ = "Unable to resolve contact points";
  }

  if (resolver->is_ok() && !contact_points.empty()) {
    remaining_connector_count_ = contact_points.size();
    for (AddressVec::iterator it = contact_points.begin(), end = contact_points.end(); it != end;
         ++it) {
      Host::Ptr host(new Host(*it));
      ControlConnector* connector = new ControlConnector(host, protocol_version_,
                                                         bind_callback(&ClusterConnector::on_connect, this));
      pending_connectors_.push_back(connector);
      connector->with_metrics(settings_.metrics)
                ->with_settings(settings_.control_connection_settings)
                ->with_listener(NULL) // Will be set by Cluster
                ->connect(event_loop_->loop());
    }
  } else {
    finish();
  }
}

void ClusterConnector::on_connect(ControlConnector* connector) {
  pending_connectors_.remove(connector);

  if (cluster_ || is_canceled()) {
    maybe_finish();
    return;
  }

  if (connector->is_ok()) {
    cluster_.reset(new Cluster(connector->release_connection(), listener_, event_loop_,
                               connector->host(), connector->hosts(),
                               connector->schema(), settings_.load_balancing_policy,
                               settings_.load_balancing_policies, settings_.local_dc,
                               connector->supported_options(), settings_));
    maybe_finish();
  } else {
    if (error_code_ == CLUSTER_OK) {
      error_code_ = CLUSTER_ERROR_CONTROL_CONNECTION;
      error_message_ = connector->error_message();
    }

    // Cleanup any policy handles if this was a failure before Cluster took ownership
    // In this driver, policies are often shared or managed elsewhere,
    // but we should ensure they are closed if they were newly created for this attempt.
    // However, in ClusterConnector, they usually come from settings_.

    maybe_finish();
  }
}

void ClusterConnector::maybe_finish() {
  if (remaining_connector_count_ > 0) {
    if (--remaining_connector_count_ == 0) {
      finish();
    }
  }
}

void ClusterConnector::finish() {
  if (resolver_) resolver_->cancel();
  for (ControlConnectorList::iterator it = pending_connectors_.begin(),
                                       end = pending_connectors_.end();
       it != end; ++it) {
    (*it)->cancel();
  }
  pending_connectors_.clear();
  callback_(this);
  dec_ref();
}

}}} // namespace datastax::internal::core

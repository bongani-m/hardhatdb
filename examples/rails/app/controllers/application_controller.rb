class ApplicationController < ActionController::Base
  around_action :route_cluster

  private

  # Reads and writes use one node for this session. A broken connection
  # moves the session to the next address.
  def route_cluster
    roles = Cluster.roles
    start = session_index(roles)
    failure = nil
    roles.each_index do |offset|
      role = roles[(start + offset) % roles.size]
      begin
        Current.node = Cluster.label(role)
        result = nil
        ActiveRecord::Base.connected_to(role: role) { result = yield }
        session[:mysql_role] = role.to_s
        return result
      rescue StandardError => error
        failure = error
        raise unless Cluster.connection_error?(error)
        raise if offset == roles.size - 1
      end
    end
    raise failure
  end

  def session_index(roles)
    if (name = session[:mysql_role])
      index = roles.index(name.to_sym)
      return index if index
    end
    Cluster.next_index
  end
end

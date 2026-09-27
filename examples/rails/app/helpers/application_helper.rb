module ApplicationHelper
  def cluster_nodes
    Cluster.roles.map { |role| node_status(role) }
  end

  private

  def node_status(role)
    addr = Cluster.label(role)
    row = ActiveRecord::Base.connected_to(role: role) do
      ActiveRecord::Base.connection.select_one("SHOW RAFT STATUS")
    end
    { addr: addr, row: stringify_row(row), error: nil, current: addr == Current.node }
  rescue StandardError => error
    { addr: addr, row: {}, error: error.message, current: addr == Current.node }
  end

  def stringify_row(row)
    return {} if row.nil?

    row.to_h.transform_keys(&:to_s)
  end
end

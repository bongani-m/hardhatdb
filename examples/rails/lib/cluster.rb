# Every address accepts reads and writes. A browser session stays on one node
# so the next page sees that write. A broken connection tries the next address.
class Cluster
  def self.addrs
    raw = ENV.fetch("MYSQL_ADDRS", "")
    list = raw.split(",").map(&:strip).reject(&:empty?)
    return list unless list.empty?

    host = ENV.fetch("MYSQL_HOST", "127.0.0.1")
    port = ENV.fetch("MYSQL_PORT", "3306")
    [ "#{host}:#{port}" ]
  end

  def self.roles
    addrs.each_index.map { |i| i.zero? ? :writing : :"node_#{i}" }
  end

  # Role names passed to connected_to, and the database.yml entry for each.
  def self.connects_to
    roles.to_h { |role| [ role, role == :writing ? :primary : role ] }
  end

  def self.label(role)
    addrs.fetch(roles.index(role))
  end

  def self.next_index
    @mutex ||= Mutex.new
    @mutex.synchronize do
      @index = @index.to_i
      current = @index % roles.size
      @index += 1
      current
    end
  end

  def self.connection_error?(error)
    case error
    when ActiveRecord::ConnectionNotEstablished,
         ActiveRecord::ConnectionFailed,
         Mysql2::Error::ConnectionError
      true
    when ActiveRecord::StatementInvalid
      connection_error?(error.cause)
    else
      false
    end
  end
end

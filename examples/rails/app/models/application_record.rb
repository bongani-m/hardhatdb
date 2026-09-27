class ApplicationRecord < ActiveRecord::Base
  primary_abstract_class

  connects_to database: Cluster.connects_to
end

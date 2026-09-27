class Note < ApplicationRecord
  self.table_name = "notes"
  self.record_timestamps = false

  belongs_to :account, inverse_of: :notes

  validates :body, presence: true

  before_validation do
    self.created_at ||= Time.now.utc
  end
end

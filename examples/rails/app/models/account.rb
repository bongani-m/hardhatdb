class Account < ApplicationRecord
  self.table_name = "accounts"
  self.record_timestamps = false

  has_many :notes, foreign_key: :account_id, inverse_of: :account, dependent: :delete_all

  validates :name, :email, presence: true

  before_validation :apply_defaults, on: :create

  def tags_text
    Array(parsed_tags).join(", ")
  end

  def tags_text=(value)
    self.tags = value.to_s.split(",").map(&:strip).reject(&:empty?)
  end

  def parsed_tags
    value = tags
    value = JSON.parse(value) if value.is_a?(String)
    Array(value)
  end

  private

  def apply_defaults
    self.created_at ||= Time.now.utc
    self.status = 1 if status.nil?
    self.tags = [] if tags.nil?
  end
end

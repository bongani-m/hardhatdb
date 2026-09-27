class Person < ApplicationRecord
  self.table_name = "mytable"

  validates :name, :email, presence: true

  def phone_numbers_text
    Array(phone_numbers).join(", ")
  end

  def phone_numbers_text=(value)
    self.phone_numbers = value.to_s.split(",").map(&:strip).reject(&:empty?)
  end
end

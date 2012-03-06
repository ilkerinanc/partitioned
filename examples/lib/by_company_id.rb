class ByCompanyId < Partitioned::ByForeignKey
  self.abstract_class = true

  def self.partition_foreign_key
    return :company_id
  end

  partitioned do |partition|
    partition.index :id, :unique => true
  end
end

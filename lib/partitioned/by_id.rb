module Partitioned
  #
  # table partitioning by id.  this partitioning breaks up data by
  # the value of its primary key.  a specific record's child table
  # is determined by the number resulting from the integer math:
  #   ID / ById::partition_table_size * ById::partition_table_size
  # 
  class ById < ByIntegerField
    self.abstract_class = true

    #
    # specific to this partitioning, we need to prefetch the primary key (id)
    # before we attempt to do the insert because the insert wants to know the
    # name of the specific child table to access.
    #
    def self.prefetch_primary_key?
      return true
    end

    #
    # the number of records in each child table.
    #
    def self.partition_table_size
      return 10000000
    end

    def self.partition_integer_field
      return :id
    end

    partitioned do |partition|
      partition.index :id, :unique => true
    end
  end
end

module Partitioned
  #
  # Table partitioning by id. this partitioning breaks up data by
  # the value of its primary key. A specific record's child table
  # is determined by the number resulting from the integer math:
  #   ID / ById::partition_table_size * ById::partition_table_size
  # 
  class ById < ByIntegerField
    self.abstract_class = true

    #
    # Specific to this partitioning, we need to prefetch the primary key (id)
    # before we attempt to do the insert because the insert wants to know the
    # name of the specific child table to access.
    #
    # @return [Boolean] true
    def self.prefetch_primary_key?
      return true
    end

    #
    # The number of records in each child table.
    #
    # @return [Integer] the number of rows in a partition
    def self.partition_table_size
      return 10000000
    end

    #
    # The name of the field to partition on
    #
    # @return [String] the name of the field to partition on
    def self.partition_integer_field
      return :id
    end

    partitioned do |partition|
      partition.index :id, :unique => true
    end
  end
end
